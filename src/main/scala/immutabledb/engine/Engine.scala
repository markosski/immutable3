package immutabledb.engine

import java.util.concurrent._

import immutabledb._
import immutabledb.codec._
import immutabledb.operator._
import immutabledb.storage._

import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

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

    def resolveProjectOp(query: Query, table: Table): ColumnVectorOperator => ProjectionOperator = {
        query.project match {
            case p: Project => ProjectOp.mkProjectOp(p.cols, p.limit)
            case p: ProjectAgg => {
                val opAggs = p.aggs.map { 
                    _ match {
                        case Max(col, alias) => {
                            val column = table.getColumn(col)
                            column.columnType match {
                                case ColumnType.INT | ColumnType.TINYINT => MaxDoubleAggr(col, alias.getOrElse(col + "_max"))
                                case _ => throw new Exception("Unsupported agg for this data type")
                            }
                        }
                        case Min(col, alias) => {
                            val column = table.getColumn(col)
                            column.columnType match {
                                case ColumnType.INT | ColumnType.TINYINT => MinDoubleAggr(col, alias.getOrElse(col + "_min"))
                                case _ => throw new Exception("Unsupported agg for this data type")
                            }
                        }
                        case Count(col, alias) => {
                            CountAggr(col, alias.getOrElse(col + "_count"))
                        }
                        case _ => throw new Exception("Unknown Aggregate type")
                    }
                }

                ProjectAggOp.make(opAggs, p.groupBy)
            }
        }
    }

    def execute(query: Query): Either[Throwable, Iterator[Row]] = {
        val resultQueue = new LinkedBlockingQueue[ColumnVectorBatch](1000)
        val table: Table = sm.getTable(query.table)
        val segmentCount: Int = sm.getTableSegmentCount(table.name)
        val selectOps: PTree = resolveSelectOps(query)
        val projectOp = resolveProjectOp(query, table)

        val pipeline = Pipeline(
            table,
            getColumns(query, table),
            ScanOp.mkScanOp(sm, query.table),
            selectOps,
            projectOp
        )

        logger.info(s"Execution Pipeline: $pipeline")

        val threads = (0 until segmentCount).toList.map { i =>
            Future {
                new PipelineThread(pipeline, i, resultQueue).run()
            }
        }

//        for {
//            done <- Future.sequence(threads)
//            resultToOp = new ResultQueueOp(resultQueue)
//            result <- Future(
//                Try(pipeline.projectOp(resultToOp).iterator) match {
//                    case Success(op) => Right(op)
//                    case Failure(err) => Left(err)
//                }
//            )
//        } yield result

        threads.foreach( f => {
            f.onComplete( {
                case Success(s) => s
                case Failure(err) => throw err
            })
            }
        )

        val resultToOp = new ResultQueueOp(resultQueue, threads.size)

        Try(pipeline.projectOp(resultToOp).iterator) match {
            case Success(op) => Right(op)
            case Failure(err) => Left(err)
        }
    }
}

class PipelineThread(pipeline: Pipeline, segIdx: Int, resultQueue: BlockingQueue[ColumnVectorBatch]) extends Runnable with LazyLogging {
    def runOps(op: ColumnVectorOperator, selectPipeline: PTree): ColumnVectorOperator = {
        def rec(tree: PTree): ColumnVectorOperator => ColumnVectorOperator = {
            tree match {
                case PNode(n1, n2, _) => (op: ColumnVectorOperator) => rec(n1)(rec(n2)(op))
                case PLeaf(op) => op
            }
        }
        rec(selectPipeline)(op)
    }

    def run(): Unit = {
        logger.debug(s"Started running pipeline for segment: $segIdx\n")

        val scanOp = pipeline.scanOp(pipeline.usedColumns, segIdx)

        val selOpIter = runOps(scanOp, pipeline.selectOps).iterator

        while (selOpIter.hasNext) {
            val vec = Try(selOpIter.next)

            vec match {
                case Success(v) if v.selectedInUse => {
//                    logger.info(s"resultQueue size: ${resultQueue.size}")
                    resultQueue.put(v)
                }
                case Failure(err) => throw err
            }
        }

        resultQueue.put(NullColumnVectorBatch)
        logger.debug(s"Finished running pipeline for segment: $segIdx\n")
    }
}