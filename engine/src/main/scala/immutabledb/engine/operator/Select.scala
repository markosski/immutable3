package immutabledb.operator

import immutabledb._

object SelectOp {
    def mkSelectOp(col: String, cond: SelectCondition) = new Function1[ColumnVectorOperator, SelectOp] {
        override def toString = s"col = $col, cond = $cond"
        def apply(op: ColumnVectorOperator) = {
            new SelectOp(col, cond, op)
        }
    }
}

class SelectOp(col: String, cond: SelectCondition, op: ColumnVectorOperator) extends ColumnVectorOperator {
    override def toString = s"col = $col, cond = $cond, op = $op"

    def iterator = cond match {
        case Match(values) => new SelectIteratorMatch(values)
        case GT(value) => new SelectIteratorGT(value)
        case LT(value) => new SelectIteratorLT(value)
        case EQ(value) => new SelectIteratorEQ(value)
        case _ => throw new Exception(s"Unsupported condition: $cond")
    }

    class SelectIteratorMatch(matchValues: List[String]) extends Iterator[FilledColumnVectorBatch] {
        val opIter = op.iterator
        def next = {
            val vec = opIter.next.asInstanceOf[FilledColumnVectorBatch]
            logger.debug(s"Vector size: ${vec.size}")
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case StringColumnVector(data) => {
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (!matchValues.contains(data(x))) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            // todo: how expensive is this copy?
            if (vec.selected.isEmpty) {
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
            val vec = opIter.next.asInstanceOf[FilledColumnVectorBatch]
            logger.debug(s"Vector size: ${vec.size}")
            // todo: how can we prevent looking up proper vector
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case IntColumnVector(data) => {
                    val gtVal = gt.toInt
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) < gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case TinyIntColumnVector(data) => {
                    val gtVal = gt.toByte
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) < gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            if (vec.selected.isEmpty) {
                logger.debug(s"Vector not in use")
                vec.copy(selectedInUse = false)
            }
            vec
        }

        def hasNext = opIter.hasNext
    }

    class SelectIteratorLT(gt: Double) extends Iterator[ColumnVectorBatch] {
        val opIter = op.iterator
        def next: ColumnVectorBatch = {
            val vec = opIter.next.asInstanceOf[FilledColumnVectorBatch]
            logger.debug(s"Vector size: ${vec.size}")
            // todo: how can we prevent looking up proper vector
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case IntColumnVector(data) => {
                    val gtVal = gt.toInt
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) > gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case TinyIntColumnVector(data) => {
                    val gtVal = gt.toByte
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) > gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            if (vec.selected.isEmpty) {
                logger.debug(s"Vector not in use")
                vec.copy(selectedInUse = false)
            }
            vec
        }

        def hasNext = opIter.hasNext
    }

    class SelectIteratorEQ(value: Double) extends Iterator[ColumnVectorBatch] {
        val opIter = op.iterator
        def next: ColumnVectorBatch = {
            val vec = opIter.next.asInstanceOf[FilledColumnVectorBatch]
            logger.debug(s"Vector size: ${vec.size}")

            // todo: how can we prevent looking up proper vector
            val colIdx = vec.columns.zipWithIndex.filter( x => x._1.name == col).head._2
            val colVec = vec.columnVectors(colIdx)

            colVec match {
                case IntColumnVector(data) => {
                    val gtVal = value.toInt
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) != gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case TinyIntColumnVector(data) => {
                    val gtVal = value.toByte
                    var i = 0
                    for (x <- vec.selected.iterator) {
                        if (data(x) != gtVal) vec.selected.remove(i)
                        i += 1
                    }
                }
                case _ => throw new Exception("Unsupported column vector")
            }
            if (vec.selected.isEmpty) {
                logger.debug(s"Vector not in use")
                vec.copy(selectedInUse = false)
            }
            vec
        }

        def hasNext = opIter.hasNext
    }
}