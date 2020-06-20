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
import ujson.Value

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
    def fromJsonValue(jsonValue: Value): SegmentMeta = {
        val json = ujson.read(jsonValue)
        SegmentMeta(json.obj("blockOffset").arr.map(x => x.num.toInt).toArray)
    }

    def toJsonValue(segmentMeta: SegmentMeta): Value = {
        ujson.Obj(
            "blockOffset" -> ujson.Arr(segmentMeta.blockOffsets.map(ujson.Num(_)):_*)
        )
    }

    def load(file: File): SegmentMeta = {
        val jsonValue = ujson.read(file)
        fromJsonValue(jsonValue)
    }
    def store(file: File, s: SegmentMeta) = {
        val fileWriter = new FileWriter(file)

        val json = toJsonValue(s)
        ujson.writeTo(json, fileWriter)
        fileWriter.close()
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
    private val codec = Column.getCodec(column)
    private val segmentPath = List(env.config.dataDir, tableName, s"${column.name}_$id.dat").mkString("/")
    private val segmentMetaPath = List(env.config.dataDir, tableName, s"${column.name}_$id.meta").mkString("/")
    private val segmentFile: RandomAccessFile = new RandomAccessFile(segmentPath, "rw")
    segmentFile.setLength(0) // delete contents

    // TODO: buffer should be sufficient to store compressed data
    private var blockBuffer: ByteBuffer = ByteBuffer.allocateDirect(blockSize * codec.dtype.size)
    private var recordsWritten = 0

    logger.info(s"id: $id, blockSize: $blockSize, tableName: $tableName, column: $column, blockBuffer allocation: ${blockBuffer.remaining()}")
    logger.info(s"segment env: ${env.config}")

    /**
      * Block offsets are stored as continous sequence.
      * For example idx 0 and 1 are starting byte and ending byte of 1st byte block, idx 2 and 3 - 2nd block etc.
      */
    val blockBufferOffsets: Buffer[Int] = Buffer[Int]()
    blockBufferOffsets += 0 // first block 0 at byte offset 0, if N segments were written out, the blockBufferOffsets.size will be N+1
    logger.info(s"column: ${column.name}, blockBufferOffsets.size: ${blockBufferOffsets.size}, segmentFile: ${segmentFile.length()}")

    def newSegment() = {
        new SegmentWriter(id + 1, blockSize, tableName, column)(env)
    }

    def write(x: String): Unit = {
        if (blockBufferOffsets.size > env.config.segmentSize) 
            throw new Exception("Segment full")

        if (recordsWritten < blockSize) {
            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
            recordsWritten += 1
        } else {
            flush()
            println(x)
            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
            recordsWritten += 1
        }
    }

    def flush(): Unit = {
        val position = blockBuffer.position()
        val bytes = new Array[Byte](position)
        logger.info(s"before, byteArray: ${bytes.size}, capacity: ${blockBuffer.capacity()}, position: ${position}")
        blockBuffer.position(0)
        blockBuffer.get(bytes, 0, position)
        val encoded = codec.encode(bytes).toByteArray
        segmentFile.write(encoded)

        blockBufferOffsets += blockBufferOffsets(blockBufferOffsets.size - 1) + encoded.size
        logger.info(s"Flushed segment, idx: $id, column: ${column.name}, recordsWritten: $recordsWritten, blockBufferOffsets.size: ${blockBufferOffsets.size}, segmentFile: ${segmentFile.length()}")

        blockBuffer.clear()
        recordsWritten = 0
    }

    // def write(xs: Array[String]) = {
    //     val encoded = codec.encode(xs).toByteArray()
    //     blockBuffer.clear()
    //     segmentFile.write(encoded)

    //     blockBufferOffsets += blockBufferOffsets(blockBufferOffsets.size - 1) + encoded.size
    //     logger.info(s"Flushed segment, idx: $id, column: ${column.name}, blockBufferOffsets.size: ${blockBufferOffsets.size}, segmentFile: ${segmentFile.length()}")
    // }

    def remaining: Int = {
        val rem = env.config.segmentSize - (blockBufferOffsets.size - 1)
        rem
    }

    def close() = {
        val dataPosition = blockBuffer.position()
        logger.info(s"closing, recordsWritten: ${recordsWritten}, position: ${dataPosition}")

        if (dataPosition > 0) flush()
        SegmentMeta.store(new File(segmentMetaPath), SegmentMeta(blockBufferOffsets.toArray))
        segmentFile.close
    }
}

class Segment(id: Int, segmentData: ByteBuffer, meta: SegmentMeta) extends Iterable[Array[Byte]] with LazyLogging {
    logger.debug(s"idx: $id, remaining: ${segmentData.remaining()}, blockOffsets.size: ${meta.blockOffsets.size}, blockOffsets: ${meta.blockOffsets.toList}")
    def iterator = new BlockIterator

    class BlockIterator extends Iterator[Array[Byte]] {
        segmentData.rewind
        var position = 0

        def next: Array[Byte] = {
            val startByteIdx = position
            val endByteIdx = position + 1
            val bytes = new Array[Byte](meta.blockOffsets(endByteIdx) - meta.blockOffsets(startByteIdx))
            logger.debug(s"idx: $id, startByteIdx: $startByteIdx, endByteIdx: $endByteIdx, allocated array size: ${bytes.size}, remaining: ${segmentData.remaining()}")
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