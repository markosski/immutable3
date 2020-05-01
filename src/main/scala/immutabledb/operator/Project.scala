package immutabledb.operator

import immutabledb.operator._
import immutabledb._
import scala.collection.mutable.Queue
import scala.annotation.tailrec

class ResultToOp(resultQueue: Queue[ColumnVectorBatch]) extends SelectionOperator {
    logger.debug(s"Result queue size: ${resultQueue.size}")
    def iterator = resultQueue.iterator
}

object ProjectOp {
    def mkProjectOp(cols: List[String], limit: Int = 0) = 
        (op: SelectionOperator) => new ProjectOp(cols, op, limit)
}

class ProjectOp(cols: List[String], op: SelectionOperator, limit: Int = 0) extends ProjectionOperator {
    def iterator = new SelectIterator

    class SelectIterator extends Iterator[Seq[Any]] {
        val opIter = op.iterator
        var currVec: ColumnVectorBatch = null
        var currVecSize = 0
        var currVecPos = 0
        var recordCount = 0 // used for query limit
        var vectorCount = 0

        @tailrec
        final def next: Seq[Any] = {
            if (currVec == null || currVecSize == 0 || currVecPos == currVecSize - 1) {
                currVec = opIter.next
                currVecSize = currVec.size
                currVecPos = 0
                vectorCount += 1
                logger.debug(s"Next Vector currVec: $currVec, vectorCount: $vectorCount")
            }

            // Need to find cols in proper order
            val vecCols: Map[String, Int] = currVec.columns.toList.zipWithIndex
                .filter( p => cols.contains(p._1.name) )
                .map( p => (p._1.name, p._2) )
                .toMap

            // logger.debug(s"currVecPos: $currVecPos")
            if (currVec.selected(currVecPos)) {
                val rec = cols.map { col =>
                    currVec.columnVectors(vecCols(col)).data(currVecPos)
                    // logger.debug(s"Size of currVec: ${currVec.columnVectors(vecCols(col)).data.size}, column: $col")
                }
                currVecPos += 1
                recordCount += 1
                rec
            } else {
                currVecPos += 1
                next
            }
        }

        def hasNext0 = {
            if (opIter.hasNext) true
            else if (currVecPos < currVecSize - 1) true
            else false
        }

        def hasNext = {
            if (limit > 0) {
                if (recordCount < limit) hasNext0
                else false
            } else {
                hasNext0
            }
        }
    }
}