package immutabledb.storage

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import immutabledb.codec._
import immutabledb.column.Column
import immutabledb.{Config, DataType}
import util.StringUtils._

/**
  * Created by marcin1 on 3/8/17.
  */
sealed trait StatsType
case object NumericStats extends StatsType
case object StringStats extends  StatsType
case object NullStats extends  StatsType

sealed trait SegmentStats

class SegmentNumericStat(min: Double, max: Double) extends SegmentStats {
    def check(value: Double): Boolean = true
}

class SegmentStringStat(data: Array[String]) extends SegmentStats {
    def check(value: String): Boolean = true
}

class SegmentNullStat

case class SegmentMeta(blockSize: Int, blockOffsets: Array[Int]) extends Serializable


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


class SegmentWriter[A, T <: DataType[A]](id: Int, codec: Codec[A], column: Column) extends Closeable {
    /**
      * This is a counter of how many blocks are in a segment
      */
    var size: Int = 0
    val segmentFile = new RandomAccessFile(column.path / s"${column.name}_$id.dat", "r")
    var blockBuffer: ByteBuffer = ByteBuffer.allocate(Config.blockSize * codec.dtype.size)

    /**
      * Block offsets are stores as continous sequence.
      * For example idx 0 and 1 are starting byte and ending byte of 1st block, idx 2 and 3 - 2nd block etc.
      */
    val blockBufferOffsets: Array[Int] = new Array[Int](Config.blockSize * 2)
    blockBufferOffsets(0) = 0

    def write(x: String) = {
//        if (size + 1 > Config.segmentSize) throw new Exception("Segment full")
//
//        if (blockBuffer.hasRemaining) {
//            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
//        } else {
//            val encoded = codec.encode(blockBuffer.array)
//            segmentFile.write(blockBuffer.array)
//            blockBuffer.clear
//
//            blockBuffer.put(codec.dtype.valueToBytes(codec.dtype.stringToValue(x)))
//            blockBuffer.put(codec.encode(Array(x)))
//            size += 1
//
//            blockBufferOffsets(blockBufferOffsets.length) = segmentFile.
//        }
    }

    def write(xs: Array[String]) = {
        require(xs.length == Config.blockSize)

        val encoded = codec.encode(xs)

        blockBufferOffsets(blockBufferOffsets.length) = encoded.size
    }

    def remaining: Int = Config.segmentSize - size

    def close = {
        segmentFile.write(blockBuffer.array)
        segmentFile.close
    }
}


class Segment[A, T <: DataType[A]](id: Int, codec: Codec[A], column: Column) extends Iterable[Array[A]] {
    val segmentFile = new RandomAccessFile(column.path / s"${column.name}_$id.dat", "r")
    val segmentMetaFile = new File(column.path / s"${column.name}_$id.meta")

    val segmentData: ByteBuffer = segmentFile.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, segmentFile.length)

    val meta: SegmentMeta = SegmentMeta.load(segmentMetaFile)

    def iterator = new BlockIterator

    class BlockIterator extends Iterator[Array[A]] {
        segmentData.rewind
        var position = 0

        def next: Array[A] = {
            val startByte = position * 2
            val endByte = position * 2 + 1
            val bytes = new Array[Byte](meta.blockOffsets(endByte) - meta.blockOffsets(startByte))
            segmentData.get(bytes)

            val data = codec.decode(Config.blockSize, new ByteArrayInputStream(bytes))

            data
        }

        def hasNext: Boolean = {
            if (segmentData.remaining - Config.blockSize * codec.dtype.size > 0) true
            else false
        }
    }
}
