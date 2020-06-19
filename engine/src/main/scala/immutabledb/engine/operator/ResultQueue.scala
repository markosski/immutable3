package immutabledb.operator

import java.util.concurrent.{BlockingQueue, ConcurrentLinkedQueue}

import immutabledb._

class ResultQueueOp(resultQueue: BlockingQueue[Option[ColumnVectorBatch]], nullCount: Int) extends ColumnVectorOperator {
    def iterator = new ResultQueueIterator()

    class ResultQueueIterator extends Iterator[ColumnVectorBatch] {
        private var finishedCount = 0
        private var cached: Option[ColumnVectorBatch] = None
        private var countFailed = 0

        def next: ColumnVectorBatch = {
            logger.debug(s"giveNext")
            cached match {
                case Some(c) => {
                    cached = None
                    c
                }
                case None => resultQueue.take().get
            }
            // if (cached != null) {
            //     val vec = cached
            //     cached = null
            //     vec
            // } else {
            //     resultQueue.take()
            // }
        }

        def hasNext: Boolean = {
            if (cached != null) {
                true
            } else if (finishedCount < nullCount) {
                logger.debug(s"has next: ${finishedCount} vs ${nullCount}")
                resultQueue.take() match {
                    case Some(x: ColumnVectorBatch) => {
                        if (x.size == 0) hasNext
                        else cached = Some(x); true
                    }
                    case None => {
                        logger.debug(s"found Null item, resultQueue size: ${resultQueue.size}")
                        finishedCount += 1
                        hasNext
                    }
                    case Some(_) => {
                        logger.error(s"found FailedColumnVectorBatch, resultQueue size: ${resultQueue.size}")
                        countFailed += 1
                        if (countFailed == 3) {
                            throw new Exception("Encountered 3 failed segments. Terminating.")
                        }
                        hasNext
                    }
                }
            } else {
                logger.debug(s"does not have next: ${finishedCount} vs ${nullCount}")
                false
            }
        }
    }
}
