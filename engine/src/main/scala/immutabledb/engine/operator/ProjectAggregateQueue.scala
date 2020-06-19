package immutabledb.engine.operator

import java.util.concurrent.{BlockingQueue, ConcurrentLinkedQueue}
import immutabledb._
import immutabledb.operator._
import immutabledb.operator.OperatorAliasses._
import scala.collection.mutable

class ProjectAggregateQueueOp(resultQueue: BlockingQueue[Option[AggMapTuple]], nullCount: Int) extends ProjectionOperator {
    logger.info(s"Current result queue vector count: ${resultQueue.size}")

    def iterator = new ProjectAggregateQueueIterator()

    class ProjectAggregateQueueIterator extends Iterator[Row] {
        private var finishedCount = 0
        private val resultMap = mutable.LinkedHashMap[String, AggMap]()

        init()
        private val resultMapIter = resultMap.iterator

        def init() = {
            while (finishedCount < nullCount) {
                resultQueue.take() match {
                    case Some(aggMapTuple) => {
                        resultMap.get(aggMapTuple._1) match {
                            case Some(v) => {
                                for ((key, entry) <- aggMapTuple._2) {
                                    (entry, v(key)) match {
                                        case (aggA: CountAggr, aggB: CountAggr) => v.put(key, aggA.combine(aggB))
                                        case (aggA: MaxDoubleAggr, aggB: MaxDoubleAggr) => v.put(key, aggA.combine(aggB))
                                        case (aggA: MaxStringAggr, aggB: MaxStringAggr) => v.put(key, aggA.combine(aggB))
                                        case (aggA: MinDoubleAggr, aggB: MinDoubleAggr) => v.put(key, aggA.combine(aggB))
                                        case (aggA: MinStringAggr, aggB: MinStringAggr) => v.put(key, aggA.combine(aggB))
                                        case (aggA: AvgDoubleAggr, aggB: AvgDoubleAggr) => v.put(key, aggA.combine(aggB))
                                        case _ => throw new Exception("Bad match")
                                    }
                                }
                                resultMap.put(aggMapTuple._1, v)
                            }
                            case None => resultMap.put(aggMapTuple._1, aggMapTuple._2)
                        }
                    }
                    case None => finishedCount += 1 // expecting null
                }
            }
            logger.info(s"resultMap size: ${resultMap.size}")
        }

        def next = {
            Row.fromSeq(resultMapIter.next._2.map( t => t._2.repr ).toList)
        }

        def hasNext = resultMapIter.hasNext
    }
}