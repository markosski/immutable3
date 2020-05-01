package immutabledb.operator

import immutabledb._

object SelectOp {
    def mkSelectOp(col: String, cond: SelectCondition) = (op: SelectionOperator) => new SelectOp(col, cond, op)
    def foo(select: SelectADT) = ???
}

class SelectOp(col: String, cond: SelectCondition, op: SelectionOperator) extends SelectionOperator {
    def iterator = cond match {
        case Match(values) => new SelectIteratorMatch(values)
        case GT(gt) => new SelectIteratorGT(gt)
        case _ => throw new Exception(s"Unsupported condition: $cond")
    }

    class SelectIteratorMatch(matchValues: List[String]) extends Iterator[ColumnVectorBatch] {
        val opIter = op.iterator
        def next: ColumnVectorBatch = {
            val vec = opIter.next
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case StringColumnVector(data) => {
                    var i = 0
                    for (x <- data) {
                        if (!matchValues.contains(x)) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            // todo: how expensive is this copy?
            if (vec.selected.size == 0) {
                logger.debug(s"Vector not in use")
                vec.copy(selectedInUse = false)
            }
            vec
        }

        def hasNext = opIter.hasNext
    }

    class SelectIteratorGT(gt: Double) extends Iterator[ColumnVectorBatch] {
        val opIter = op.iterator
        def next: ColumnVectorBatch = {
            val vec = opIter.next
            // todo: how can we prevent looking up proper vector
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case IntColumnVector(data) => {
                    val gtVal = gt.toInt
                    var i = 0
                    for (x <- data) {
                        if (x < gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case TinyIntColumnVector(data) => {
                    val gtVal = gt.toByte
                    var i = 0
                    for (x <- data) {
                        if (x < gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            if (vec.selected.size == 0) {
                logger.debug(s"Vector not in use")
                vec.copy(selectedInUse = false)
            }
            vec
        }

        def hasNext = opIter.hasNext
    }
}