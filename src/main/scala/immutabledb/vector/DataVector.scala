package immutabledb.vector

/**
  * Created by marcin1 on 7/12/17.
  */
import immutabledb.column.Column
import scala.collection.mutable
import immutabledb.{DataType}

trait VectorBatch

case class ColumnVectorBatch(
                             oid: Int,
                             size: Int,
                             selectedInUse: Boolean,
                             columnVectors: Array[ColumnVector],
                             columns: Array[Column],
                             selected: Array[Int]
                     ) extends VectorBatch

trait ColumnVector {
    val data: Array[_]
}

case class IntColumnVector(data: Array[Int]) extends ColumnVector

case class ByteColumnVector(data: Array[Byte]) extends ColumnVector

case class StringColumnVector(data: Array[String]) extends ColumnVector

object NullColVector extends VectorBatch

//class DataVectorProducer(table: Table, cols: List[Column]) extends Iterable[DataVector] {
//    def iterator = new DataVectorIterator()

//    class DataVectorIterator extends Iterator[DataVector] {
//        var vecCounter = -1  // because we want it to start at 0
//        val encIters = cols.map(x => {
//            SelectionOperator.prepareBuffer(
//                x,
//                SchemaManager.getTable(x.tblName)
//            )
//            x.getIterator
//        })
//
//        def next = {
//            vecCounter += 1
//            new DataVector(vecCounter, cols, encIters.map(x => x.next))
//        }
//
//        def hasNext = {
//            if (vecCounter * Config.vectorSize >= table.size) false else true
//        }
//    }
//}
