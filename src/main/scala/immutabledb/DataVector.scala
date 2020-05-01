package immutabledb

/**
  * Created by marcin1 on 7/12/17.
  * 
  */
import immutabledb._
import immutabledb.storage._
import immutabledb.codec._
import scala.collection.mutable
import java.io.ByteArrayInputStream
import com.typesafe.scalalogging.LazyLogging
import immutabledb.operator.SelectionOperator

case class ColumnVectorBatch(
                             oid: Int,
                             size: Int,
                             columnVectors: Array[ColumnVector],
                             columns: Array[Column],
                             selected: mutable.BitSet,
                             selectedInUse: Boolean // if not matching records set to false
                     )

trait ColumnVector {
    val data: Array[_]
}

case class IntColumnVector(data: Array[Int]) extends ColumnVector
case class TinyIntColumnVector(data: Array[Byte]) extends ColumnVector
case class StringColumnVector(data: Array[String]) extends ColumnVector

// object NullColVector extends ColumnVector

/*
DataVectorProducer should be created for single segment.

import immutabledb.vector._
import immutabledb.column._
import immutabledb.storage._
import immutabledb.codec._

val sm = new SegmentManager("/Users/marcin/immutable3")
val colAge = new Column("age", CodecType.DENSE_TINYINT)
val vec = new DataVectorProducer(sm, "test1", List(colAge))
*/
class DataVectorProducer0(sm: SegmentManager, tableName: String, cols: List[Column]) extends Iterable[ColumnVectorBatch] with LazyLogging {
  def iterator = new DataVectorIterator()

  class DataVectorIterator extends Iterator[ColumnVectorBatch] {
    private var vecCounter = -1  // because we want it to start at 0
    var currSegmentIdx = 0
    val table = sm.tables.filter(t => t.name == tableName).head
    val segmentIters: List[List[Segment#BlockIterator]] = cols
      .map(c => sm.getSegments(tableName, c.name).map(s => s.iterator))
    val segmentCount = segmentIters(0).size

    def next = {
      vecCounter += 1
      val columnVectors = new Array[ColumnVector](cols.size)

      for (cIdx <- 0 until cols.size) {
        val segment = segmentIters(cIdx)(currSegmentIdx)
        val codec = Column.getCodec(cols(cIdx))
        val bytes = segment.next
        logger.debug(s"next:: Block size: ${bytes.size}")

        codec match {
          case PFORCodecInt => {
            columnVectors(cIdx) = IntColumnVector(PFORCodecInt.decode(new ByteArrayInputStream(bytes)))
          }
          case DenseCodecInt => {
            columnVectors(cIdx) = IntColumnVector(DenseCodecInt.decode(new ByteArrayInputStream(bytes)))
          }
          case DenseCodecTinyInt => {
            columnVectors(cIdx) = TinyIntColumnVector(DenseCodecTinyInt.decode(new ByteArrayInputStream(bytes)))
          }
          case c: DenseCodecString => {
            columnVectors(cIdx) = StringColumnVector(c.decode(new ByteArrayInputStream(bytes)))
          }
          case _ => throw new Exception(s"No implementation for $codec")
        }
      }

      val vecSize = columnVectors(0).data.size // based on first column
      val bitSet = mutable.BitSet()
      for (x <- 0 until vecSize) { bitSet.add(x) } // set all bits

      ColumnVectorBatch(
        vecCounter * table.blockSize, 
        vecSize, 
        columnVectors,
        cols.toArray,
        bitSet,
        true
      )
    }

    def hasNext = {
      if (segmentIters(0)(currSegmentIdx).hasNext) {
        true
      } else if (currSegmentIdx < segmentCount - 1) {
        logger.debug(s"hasNext:: currSegmentIdx: $currSegmentIdx, segmentCount: $segmentCount")
        currSegmentIdx += 1
        true
      } else {
        false
      }
    }
  }
}