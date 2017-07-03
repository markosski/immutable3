package immutabledb.codec

import java.nio.ByteBuffer

import immutabledb.{DataType, IntType}
import me.lemire.integercompression.differential.IntegratedIntCompressor

import scala.reflect.ClassTag

/**
  * Created by marcin1 on 7/28/17.
  */
//class PFORCodec[T <: IntType.type](val dtype: T)(implicit tag: ClassTag[T#A]) extends Codec[T] {
//    val iic = new IntegratedIntCompressor()
//
//    def encode(values: Array[String]): ByteBuffer = {
//        val compressed = iic.compress(values.map(x => dtype.stringToValue(x)))
//
//        val result: ByteBuffer = ByteBuffer.allocate((compressed.length * dtype.size) + 8)
//
//        for (i <- compressed.indices) result.putInt(compressed(i))
//
//        result.flip
//        result
//    }
//
//    def decode(dataSize: Int, data: ByteBuffer): Array[T#A] = {
//        val intData = new Array[Int](dataSize)
//        for (i <- intData.indices) {
//            intData(i) = data.getInt
//        }
//
//        iic.uncompress(intData)
//    }
//}
