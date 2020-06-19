package immutabledb.engine

import java.util.concurrent._

import immutabledb._
import immutabledb.codec._
import immutabledb.operator._
import immutabledb.storage._
import immutabledb.engine.operator.{ProjectAggregateQueueOp}
import immutabledb.operator.OperatorAliasses._

import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

// - execute selection operator accumulating vectors in _result_queue_
// - execute projection operator from _result_queue_
/*

import java.util.concurrent.ExecutorService
import immutabledb._
import immutabledb.engine._
import immutabledb.storage._
import immutabledb.operator._

implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

val th = java.util.concurrent.Executors.newFixedThreadPool(1)
val sm = new SegmentManager(DevEnv.config.dataDir)
val table = sm.getTable("test_100m")
val engine = new Engine(sm, th)

val query = Query(
    table.name, 
    And(
        Select("age", GT(0)),
        Select("state", Match(List("DC", "CT")))
    ),
    Project(List("age", "state"), 10)
)

val query = Query(
    table.name, 
    Select("age", GT(30)),
    Project(List("age", "state", "id"), 10)
)

val res = engine.execute(query)
res.onSuccess { case r => r.right.get.foreach(println) }
res.onSuccess { case r => r.foreach(println) }

import java.util.concurrent.ExecutorService
import immutabledb._
import immutabledb.engine._
import immutabledb.storage._
import immutabledb.operator._

implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

val th = java.util.concurrent.Executors.newFixedThreadPool(4)
val sm = new SegmentManager(DevEnv.config.dataDir)
val table = sm.getTable("test_100m")
val engine = new Engine(sm, th)

val query = Query(
    table.name, 
        Select("age", GT(18)),
            ProjectAgg(List(Min("age"), Max("age")), List("state"))
)
val res = engine.execute(query)
res.onSuccess { case r => r.right.get.foreach(println) }
res.onSuccess { case r => r.left.get.printStackTrace }

*/
class Engine(sm: SegmentManager, es: ExecutorService) extends LazyLogging {
    implicit val ec = ExecutionContext.fromExecutorService(es)

    // Collect all columns that will be used in the query pipeline
    def getColumns(query: Query, table: Table): List[Column] = {
        def rec(sel: SelectADT): Set[Column] = {
            sel match {
                case And(a, b) => rec(a) ++ rec(b)
                case Or(a, b) => rec(a) ++ rec(b)
                case Select(col, cond) => Set(table.getColumn(col))
                case NoSelect => Set()
            }
        }

        val projectColumns: List[Column] = query.project match {
            case Project(cols, limit) => cols.map(c => table.getColumn(c))
            case ProjectAgg(aggs, groupBy) => {
                val aggsCols = aggs.map(a => a.col).map(c => table.getColumn(c))
                val groupCols = groupBy.map(c => table.getColumn(c))
                aggsCols ++ groupCols
            }
        }

        // there was an issue if there is duplicate column in the list, try to understand why it breaks?
        (rec(query.select).toList ++ projectColumns).toSet.toList
    }

    def resolveSelectOps(query: Query): PTree = {
        def rec(sel: SelectADT): PTree = {
            sel match {
                case And(a, b) => {
                    PNode(
                        rec(a), 
                        rec(b), 
                        PipelineOp.AND)
                }
                case Or(a, b) => {
                    PNode(
                        rec(a), 
                        rec(b), 
                        PipelineOp.OR)
                }
                case Select(col, cond) => PLeaf(SelectOp.mkSelectOp(col, cond))
                case NoSelect => PLeaf(identity[ColumnVectorOperator])
            }
        }
        rec(query.select)
    }

    def resolveProjectOp(projectAgg: ProjectAgg, table: Table): ColumnVectorOperator => Operator[AggMapTuple] = {
        val aggs = projectAgg.aggs.map { 
            _ match {
                case Max(col, alias) => {
                    val column = table.getColumn(col)
                    column.columnType match {
                        case ColumnType.INT | ColumnType.TINYINT => MaxDoubleAggr(col, alias.getOrElse(col + "_max"))
                        case ColumnType.STRING => MaxStringAggr(col, alias.getOrElse(col + "_max"))
                        case _ => throw new Exception("Unsupported agg for this data type")
                    }
                }
                case Min(col, alias) => {
                    val column = table.getColumn(col)
                    column.columnType match {
                        case ColumnType.INT | ColumnType.TINYINT => MinDoubleAggr(col, alias.getOrElse(col + "_min"))
                        case ColumnType.STRING => MaxStringAggr(col, alias.getOrElse(col + "_min"))
                        case _ => throw new Exception("Unsupported agg for this data type")
                    }
                }
                case Count(col, alias) => {
                    CountAggr(col, alias.getOrElse(col + "_count"))
                }
                case _ => throw new Exception("Unknown Aggregate type")
            }
        }
        ProjectAggOp.make(aggs, projectAgg.groupBy)
    }

    def execute(query: Query): Either[Throwable, Iterator[Row]] = {
        // val resultQueue = new LinkedBlockingQueue[ColumnVectorBatch](100)
        val table: Table = sm.getTable(query.table)
        val segmentCount: Int = sm.getTableSegmentCount(table.name)
        val selectOps: PTree = resolveSelectOps(query)

        query.project match {
            case Project(cols, limit) => {
                val resultQueue = new LinkedBlockingQueue[Option[ColumnVectorBatch]](100)
                val pipeline = Pipeline(
                    table,
                    getColumns(query, table),
                    ScanOp.mkScanOp(sm, query.table),
                    selectOps,
                    ProjectOp.mkProjectOp(cols, limit)
                )
                logger.info(s"Execution Pipeline: $pipeline")
                
                val threads = (0 until segmentCount).toList.map { i =>
                    Future {
                        new PipelineThread(pipeline, i, resultQueue).run()
                    }
                }

                threads.foreach( f => {
                    f.onComplete( {
                        case Success(s) => s
                        case Failure(err) => throw err
                    })
                    }
                )

                val resultToOp = new ResultQueueOp(resultQueue, threads.size)
                val projectIter = pipeline.projectOp(resultToOp)

                Try(projectIter.iterator) match {
                    case Success(op) => Right(op)
                    case Failure(err) => Left(err)
                }

            }
            case p: ProjectAgg => {
                val resultQueue = new LinkedBlockingQueue[Option[AggMapTuple]](100)
                val pipeline = Pipeline(
                    table,
                    getColumns(query, table),
                    ScanOp.mkScanOp(sm, query.table),
                    selectOps,
                    resolveProjectOp(p, table)
                )
                logger.info(s"Execution Pipeline: $pipeline")

                val threads = (0 until segmentCount).toList.map { i =>
                    Future {
                        new PipelineAggregateThread(pipeline, i, resultQueue).run()
                    }
                }

                threads.foreach( f => {
                    f.onComplete( {
                        case Success(s) => s
                        case Failure(err) => throw err
                    })
                    }
                )

                val resultToOp = new ProjectAggregateQueueOp(resultQueue, threads.size)

                Try(resultToOp.iterator) match {
                    case Success(op) => Right(op)
                    case Failure(err) => Left(err)
                }
            }
        }
    }
}

class PipelineThread(pipeline: Pipeline[_], segIdx: Int, resultQueue: BlockingQueue[Option[ColumnVectorBatch]]) extends Runnable with LazyLogging {
    // TODO: use AND/OR operators
    def runOps(op: ColumnVectorOperator, selectPipeline: PTree): Operator[ColumnVectorBatch] = {
        def rec(tree: PTree): ColumnVectorOperator => ColumnVectorOperator = {
            tree match {
                case PNode(n1, n2, _) => (x: ColumnVectorOperator) => rec(n2)(rec(n1)(x))
                case PLeaf(op0) => op0
            }
        }
        rec(selectPipeline)(op)
    }

    def run(): Unit = {
        logger.debug(s"Started running pipeline for segment: $segIdx\n")

        val scanOp = pipeline.scanOp(pipeline.usedColumns, segIdx)
        val mainOpIter = runOps(scanOp, pipeline.selectOps).iterator

        while (mainOpIter.hasNext) {
            Try(mainOpIter.next) match {
                case Success(v) => resultQueue.put(Some(v))
                case Failure(err) => throw err
            }
        }

        resultQueue.put(None) // to represent end of elements
        logger.debug(s"Finished running pipeline for segment: $segIdx\n")
    }
}

class PipelineAggregateThread(pipeline: Pipeline[AggMapTuple], segIdx: Int, resultQueue: BlockingQueue[Option[AggMapTuple]]) extends Runnable with LazyLogging {
    // TODO: use AND/OR operators
    def runOps(op: ColumnVectorOperator, selectPipeline: PTree, projector: ColumnVectorOperator => Operator[AggMapTuple]): Operator[AggMapTuple] = {
        def rec(tree: PTree): ColumnVectorOperator => ColumnVectorOperator = {
            tree match {
                case PNode(n1, n2, _) => (x: ColumnVectorOperator) => rec(n2)(rec(n1)(x))
                case PLeaf(op0) => op0
            }
        }
        projector(rec(selectPipeline)(op))
    }

    def run(): Unit = {
        logger.debug(s"Started running pipeline for segment: $segIdx\n")

        val scanOp = pipeline.scanOp(pipeline.usedColumns, segIdx)
        val mainOpIter = runOps(scanOp, pipeline.selectOps, pipeline.projectOp).iterator

        while (mainOpIter.hasNext) {
            Try(mainOpIter.next) match {
                case Success(v) => resultQueue.put(Some(v))
                case Failure(err) => throw err
            }
        }

        resultQueue.put(None) // to represent end of elements
        logger.debug(s"Finished running pipeline for segment: $segIdx\n")
    }
}