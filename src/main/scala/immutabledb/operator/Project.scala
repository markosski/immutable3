package immutabledb.operator

import immutabledb.operator._
import immutabledb._
import scala.collection.mutable.Queue
import scala.annotation.tailrec

object ProjectOp {
    def mkProjectOp(cols: List[String], limit: Int = 0) = 
        (op: ColumnVectorOperator) => new ProjectOp(cols, op, limit)
}

class ProjectOp(cols: List[String], op: ColumnVectorOperator, limit: Int = 0) extends ProjectionOperator {
    override def toString = s"cols = $cols, op = $op, limit = $limit"

    def iterator = new ProjectIterator

    class ProjectIterator extends Iterator[Row] {
        private val opIter = op.iterator
        private var currVec: ColumnVectorBatch = null
        private var currVecSize = 0
        private var currRecordCount = 0
        private var totalRecordCount = 0 // used for query limit
        private var currVecPos = 0
        private var vectorCount = 0

        // Need to find cols in proper order
        lazy val vecCols: Map[String, Int] = currVec.columns.toList.zipWithIndex
            .filter( p => cols.contains(p._1.name) )
            .map( p => (p._1.name, p._2) )
            .toMap

        def next: Row = {
            logger.debug(s"give next, vectorCount: $vectorCount, currVecPos: $currVecPos, currVecSize: $currVecSize")
            if (currVec == null || currRecordCount == currVec.selected.size) {
                currVec = opIter.next
                currVecSize = currVec.size
                currVecPos = 0
                currRecordCount = 0
                vectorCount += 1
                logger.debug(s"Next Vector currVec: $currVec, vectorCount: $vectorCount")
            }

            // skip over non selected
            // potentially can be further optimized byt iterating over selected
            while (!currVec.selected(currVecPos) && currVecPos < currVec.size) {
                currVecPos += 1
                logger.debug(s"Advance currVecPos: $currVecPos")
            }

            val rec = cols.map { col =>
                currVec.columnVectors(vecCols(col)).data(currVecPos)
            }

            currRecordCount += 1
            totalRecordCount += 1
            currVecPos += 1
            logger.debug(s"rec : $rec")
            Row.fromSeq(rec)
        }

        def hasNext0 = {
            logger.debug(s"hasNext0")
            if (currVec != null && currRecordCount < currVec.selected.size) true
            else if (opIter.hasNext) true
            else false
        }

        def hasNext = {
            if (limit > 0) {
                if (totalRecordCount < limit) hasNext0
                else false
            } else {
                hasNext0
            }
        }
    }
}