package immutabledb.operator

import immutabledb._
import immutabledb.storage._
import immutabledb.codec._
import scala.collection.mutable
import java.io.ByteArrayInputStream
import com.typesafe.scalalogging.LazyLogging

object ScanOp {
    def mkScanOp(sm: SegmentManager, tableName: String) = {
        (cols: List[Column], segIdx: Int) => new ScanOp(sm, segIdx, tableName, cols)
    }
}

class ScanOp(sm: SegmentManager, segIdx: Int, tableName: String, cols: List[Column]) extends ColumnVectorOperator {
  def iterator = new DataVectorIterator()

  override def toString = s"segIdx = $segIdx, tableName = $tableName, cols = $cols"

  class DataVectorIterator extends Iterator[FilledColumnVectorBatch] {
    var vecCounter = 0
    val table = sm.tables.filter(t => t.name == tableName).head
    val segmentIters: Vector[Segment#BlockIterator] = cols
      .map(c => sm.getSegments(tableName, c.name)(segIdx).iterator).toVector

    def next = {
      val columnVectors = new Array[ColumnVector](cols.size)

      for (cIdx <- 0 until cols.size) {
        val codec = Column.getCodec(cols(cIdx))
        val bytes = segmentIters(cIdx).next
        logger.debug(s"next:: Block/Vector size in bytes: ${bytes.size}")

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

      logger.debug(s"Finished scans for $cols")

      val vecSize = columnVectors(0).data.size // based on first column
      val bitSet = mutable.BitSet()
      for (x <- 0 until vecSize) { bitSet.add(x) } // set all bits

      val vec = FilledColumnVectorBatch(
        vecCounter * table.blockSize, 
        vecSize, 
        columnVectors,
        cols.toArray,
        bitSet,
        true
      )
      logger.debug(s"New vector created: $vec")
      vecCounter += 1
      vec
    }

    def hasNext = segmentIters.head.hasNext // should not matter for which column, use first
  }
}