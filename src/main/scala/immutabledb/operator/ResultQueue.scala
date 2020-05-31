package immutabledb.operator

import java.util.concurrent.{BlockingQueue, ConcurrentLinkedQueue}

import immutabledb._

class ResultQueueOp(resultQueue: BlockingQueue[ColumnVectorBatch], nullCount: Int) extends ColumnVectorOperator {
    logger.info(s"Current result queue vector count: ${resultQueue.size}")

    def iterator = new ResultQueueIterator()

    class ResultQueueIterator extends Iterator[ColumnVectorBatch] {
        private var finishedCount = 0
        private var cached: ColumnVectorBatch = null
        private var countFailed = 0

        def next: ColumnVectorBatch = {
            logger.debug(s"giveNext")
            if (cached != null) {
                val vec = cached
                cached = null
                vec
            } else {
                resultQueue.take()
            }
        }

        def hasNext: Boolean = {
            if (cached != null) {
                true
            } else if (finishedCount < nullCount) {
                logger.debug(s"has next: ${finishedCount} vs ${nullCount}")
                resultQueue.take() match {
                    case NullColumnVectorBatch => {
                        logger.debug(s"found NullColumnVectorBatch, resultQueue size: ${resultQueue.size}")
                        finishedCount += 1
                        hasNext
                    }
                    case FailedColumnVectorBatch => {
                        logger.error(s"found FailedColumnVectorBatch, resultQueue size: ${resultQueue.size}")
                        countFailed += 1
                        if (countFailed == 3) {
                            throw new Exception("Encountered 3 failed segments. Terminating.")
                        }
                        hasNext
                    }
                    case x: ColumnVectorBatch => {
                        if (x.size == 0) hasNext
                        else cached = x; true
                    }
                }
            } else {
                logger.debug(s"does not have next: ${finishedCount} vs ${nullCount}")
                false
            }
        }
    }
}
