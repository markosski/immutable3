package immutabledb.storage

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.collection.mutable.Buffer

import immutabledb._
import immutabledb.codec._
import immutabledb.{ConfigEnv, DataType}
import util.StringUtils._
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by marcin1 on 3/8/17.
  */
sealed trait StatsType
case object NumericStats extends StatsType
case object StringStats extends  StatsType
case object NullStats extends  StatsType

sealed trait SegmentStats
class SegmentNullStat
class SegmentNumericStat(min: Double, max: Double) extends SegmentStats {
    def check(value: Double): Boolean = true
}
class SegmentStringStat(min: String, max: String) extends SegmentStats {
    def check(value: String): Boolean = true
}

// Consider storing information about how many records are stored
case class SegmentMeta(blockOffsets: Array[Int]) extends Serializable

object SegmentMeta {
    def load(file: File): SegmentMeta = {
        val fileIn = new FileInputStream(file)
        val objectIn = new ObjectInputStream(fileIn)
        val s = objectIn.readObject().asInstanceOf[SegmentMeta]
        objectIn.close
        fileIn.close
        s
    }
    def store(file: File, s: SegmentMeta) = {
        val fileOut = new FileOutputStream(file)
        val objectOut = new ObjectOutputStream(fileOut)
        objectOut.writeObject(s)
        objectOut.close
        fileOut.close
    }
}

/*
import immutabledb._
import immutabledb.column._
import immutabledb.codec._
import immutabledb.storage._

val s = new SegmentWriter(0, new DenseCodec(IntType), Column("a", "b"))(DevEnv)
s.write(Array("1", "2", "3", "4", "5"))
s.close()
*/
class SegmentWriter[E <: ConfigEnv](id: Int, blockSize: Int, tableName: String, column: Column)(env: E) extends Closeable with LazyLogging {
    /**
      * This is a counter of how many blocks are in a segment
      */
    val codec = Column.getCodec(column)
    val segmentPath = env.config.dataDir / tableName / s"${column.name}_$id.dat"
    val segmentMetaPath = env.config.dataDir / tableName / s"${column.name}_$id.meta"
    val segmentFile: RandomAccessFile = new RandomAccessFile(segmentPath, "rw")
    var blockBuffer: ByteBuffer = ByteBuffer.allocate(blockSize * codec.dtype.size)

    /**
      * Block offsets are stored as continous sequence.
      * For example idx 0 and 1 are starting byte and ending byte of 1st block, idx 2 and 3 - 2nd block etc.
      */
    val blockBufferOffsets: Buffer[Int] = Buffer[Int]()
    blockBufferOffsets += 0 // first block 0 at byte offset 0
    logger.info(s"column: ${column.name}, size: ${blockBufferOffsets.size}, segmentFile: ${segmentFile.length()}")

    def newSegment() = {
        new SegmentWriter(id + 1, blockSize, tableName, column)(env)
    }

    def write(x: String): Unit = {
        if (blockBufferOffsets.size + 1 > env.config.segmentSize) throw new Exception("Segment full")

        if (blockBuffer.hasRemaining) {
            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
        } else {
            flush()
            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
        }
    }

    def flush(): Unit = {
        val encoded = codec.encode(blockBuffer.array).toByteArray
        segmentFile.write(encoded)
        blockBuffer.clear

        blockBufferOffsets += blockBufferOffsets(blockBufferOffsets.size - 1) + encoded.size
        logger.info(s"column: ${column.name}, size: ${blockBufferOffsets.size}, segmentFile: ${segmentFile.length()}")
    }

    def write(xs: Array[String]) = {
        val encoded = codec.encode(xs).toByteArray()
        segmentFile.write(encoded)
        blockBufferOffsets(blockBufferOffsets.length) = encoded.size
    }

    def remaining: Int = env.config.segmentSize - blockBufferOffsets.size

    def close() = {
        flush()
        SegmentMeta.store(new File(segmentMetaPath), SegmentMeta(blockBufferOffsets.toArray))
        blockBuffer.clear
        segmentFile.close
    }
}

class Segment(id: Int, segmentData: ByteBuffer, meta: SegmentMeta) extends Iterable[Array[Byte]] with LazyLogging {
    logger.debug(s"segmentMeta blockOffsets: ${meta.blockOffsets.toList}")
    def iterator = new BlockIterator

    class BlockIterator extends Iterator[Array[Byte]] {
        segmentData.rewind
        var position = 0

        def next: Array[Byte] = {
            val startByte = position // todo: improve on naming of these variables
            val endByte = position + 1
            val bytes = new Array[Byte](meta.blockOffsets(endByte) - meta.blockOffsets(startByte))
            logger.debug(s"startByte: $startByte, endByte: $endByte, allocated array size: ${bytes.size}")
            segmentData.get(bytes)
            position += 1
            bytes
        }

        def hasNext: Boolean = {
            if (position < meta.blockOffsets.size - 1) {
                true
            } else {
                segmentData.rewind()
                false
            }
        }
    }
}