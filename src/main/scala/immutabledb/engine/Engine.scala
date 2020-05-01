package immutabledb.engine

import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor
import immutabledb._
import immutabledb.operator._
import immutabledb.storage._
import scala.collection.mutable.Queue
import scala.concurrent.Future
import java.util.concurrent.BlockingQueue
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging.LazyLogging

// - execute selection operator accumulating vectors in _result_queue_
// - execute projection operator from _result_queue_
/*

import java.util.concurrent.ExecutorService
import immutabledb._
import immutabledb.engine._
import immutabledb.storage._
import immutabledb.operator._

implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

val th = java.util.concurrent.Executors.newFixedThreadPool(4)
val sm = new SegmentManager(DevEnv.config.dataDir)
val table = sm.getTable("test1")
val query = Query(
    table.name, 
    And(
        Select("age", GT(0)),
        Select("state", Match(List("DC", "CT")))
    ),
    Project(List("age", "state"), 2)
)

val engine = new Engine(sm, th)
val res = engine.execute(query)
res.onSuccess { case r => r.foreach(println) }

*/
class Engine(sm: SegmentManager, es: ExecutorService) extends LazyLogging {
    // todo have a global object that allows running query in parallel
    implicit val ec = ExecutionContext.fromExecutorService(es)

    def getColumns(query: Query, table: Table): List[Column] = {
        def rec(sel: SelectADT): Set[Column] = {
            sel match {
                case And(a, b) => rec(a) ++ rec(b)
                case Or(a, b) => rec(a) ++ rec(b)
                case Select(col, cond) => Set(table.getColumn(col))
            }
        }
        rec(query.select).toList
    }

    def getSelectPipeline(query: Query): PTree = {
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
            }
        }
        rec(query.select)
    }

    def execute(query: Query): Future[Iterator[Seq[Any]]] = {
        val resultQueue = Queue[ColumnVectorBatch]()
        val table: Table = sm.getTable(query.table)
        val segmentCount: Int = sm.getTableSegmentCount(table.name)
        val selectOps: PTree = getSelectPipeline(query)
        val projectOp = query.project match {
            case p: Project => ProjectOp.mkProjectOp(p.cols, p.limit)
        }

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
                (new PipelineThread(pipeline, i, resultQueue)).run()
            }
        }

        for {
            done <- Future.sequence(threads)
            resultToOp = new ResultToOp(resultQueue)
            result <- Future(pipeline.projectOp(resultToOp).iterator)
        } yield result
    }
}

class PipelineThread(pipeline: Pipeline, segIdx: Int, resultQueue: Queue[ColumnVectorBatch]) extends Runnable with LazyLogging {
    def runOps(op: SelectionOperator, selectPipeline: PTree): SelectionOperator = {
        def rec(tree: PTree): SelectionOperator => SelectionOperator = { 
            tree match {
                case PNode(n1, n2, _) => (op: SelectionOperator) => rec(n1)(rec(n2)(op))
                case PLeaf(op) => op
            }
        }
        rec(selectPipeline)(op)
    }

    def run(): Unit = {
        logger.debug(Thread.currentThread.getName() + s" running pipeline for segment: $segIdx\n")

        val scanOp = pipeline.scanOp(pipeline.columns, segIdx)
        val selOpIter = runOps(scanOp, pipeline.selectOps).iterator

        while (selOpIter.hasNext) {
            val vec = selOpIter.next
            if (vec.selectedInUse) resultQueue.enqueue(vec)
        } 
    }
}