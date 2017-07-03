package immutabledb.column


import java.io.{BufferedOutputStream, Closeable, FileOutputStream}
import java.nio.ByteBuffer
import util.StringUtils._
import util.IOUtils
import immutabledb.{Config, DataType}
import immutabledb.NumericDataType


/**
  * Created by marcin1 on 3/8/17.
  * - write encoded data
  * - optionally partition by specified key
  * - optionally order by sortkey, if partitioned, sort within partition
  * - write statistics per segment
  * - how to materialize segment?
  * - maintain
  */

/**
  *
  * @param column       Instance of column
  * @param segmentSize  Number of values
  */
//class SegmentWriter(column: Column, segmentSize: Int) {
//    private var segmentNum = 0
//
//    private val path: String = Config.dataDir / column.tableName / column.name
//
//    /**
//      * Write full segment.
//      * This method should produce new segment file for each invocation.
//      *
//      * @param data
//      */
//    def write(data: Array[String]): Unit = {
//        assert(data.length == segmentSize, "Data size should be the same as specified segmentSize.")
//
//        IOUtils.createDir(path)
//
//        val out = new BufferedOutputStream(
//            new FileOutputStream(path / s"seg_$segmentNum", false),
//            Config.readBufferSize
//        )
//
//        val bytes: ByteBuffer = column.codec.encode(data)
//
//        out.write(bytes.array)
//        out.flush()
//        out.close()
//
//        segmentNum += 1

//        column.codec.dtype match {
//            case dtype: NumericDataType[T] => {
//                var i = 0
//                var min = dtype.maxVal
//                var max = dtype.minVal
//                while (i < data.length) {
//                    val currentValue = dtype.stringToValue(data(i))
//
//                    if (dtype.numOps.gt(currentValue, max)) max = currentValue
//                    else if (dtype.numOps.lt(currentValue, min)) min = currentValue
//
//                    i += 1
//                }
//
////                val seg = NumericSegment(startByte, endByte, segmentSize, min, max)
////                println(s"NumericSegment of size ${data.length} and min ${seg.min} and max ${seg.max}")
//
//            }
//            case dtype: DataType[T] => {
////                SimpleSegment(startByte, endByte, segmentSize)
////                println("This is non-numeric column")
//            }
//        }
//    }
//}
