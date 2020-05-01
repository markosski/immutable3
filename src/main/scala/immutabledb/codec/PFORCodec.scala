package immutabledb.codec

import java.nio.ByteBuffer
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import scala.collection.mutable.Buffer
import immutabledb.{DataType, IntType}
import me.lemire.integercompression.differential.IntegratedIntCompressor

import scala.reflect.ClassTag

/**
  * Created by marcin1 on 7/28/17.
  */
object PFORCodecInt extends Codec[Int] {
   val dtype = IntType

   val iic = new IntegratedIntCompressor()

   def encode(values: Array[Byte]) = {
      val compressed = iic.compress(values.sliding(dtype.size, dtype.size).toArray.map(x => dtype.bytesToValue(x)))

      val result: ByteBuffer = ByteBuffer.allocate((compressed.length * dtype.size) + 8)

      for (i <- compressed.indices) result.putInt(compressed(i))

      result.flip
      val bos = new ByteArrayOutputStream()
      bos.write(result.array())
      bos
   }

   def encode(values: Array[String]): ByteArrayOutputStream = {
      val compressed = iic.compress(values.map(x => dtype.stringToValue(x)))

      val result: ByteBuffer = ByteBuffer.allocate((compressed.length * dtype.size) + 8)

      for (i <- compressed.indices) result.putInt(compressed(i))

      result.flip
      val bos = new ByteArrayOutputStream()
      bos.write(result.array())
      bos
   }

   def decode(data: ByteArrayInputStream): Array[Int] = {
       val intData = Buffer[Int]()
       for (i <- intData.indices) {
           intData += data.read()
       }

       iic.uncompress(intData.toArray)
   }
}

// class PFORCodec[T <: IntType.type](val dtype: T)(implicit tag: ClassTag[T#A]) extends Codec[T] {
//    val iic = new IntegratedIntCompressor()

//    def encode(values: Array[Byte]) = {
//       val compressed = iic.compress(values.sliding(dtype.size, dtype.size).toArray.map(x => dtype.bytesToValue(x)))

//       val result: ByteBuffer = ByteBuffer.allocate((compressed.length * dtype.size) + 8)

//       for (i <- compressed.indices) result.putInt(compressed(i))

//       result.flip
//       val bos = new ByteArrayOutputStream()
//       bos.write(result.array())
//       bos
//    }

//    def encode(values: Array[String]): ByteArrayOutputStream = {
//       val compressed = iic.compress(values.map(x => dtype.stringToValue(x)))

//       val result: ByteBuffer = ByteBuffer.allocate((compressed.length * dtype.size) + 8)

//       for (i <- compressed.indices) result.putInt(compressed(i))

//       result.flip
//       val bos = new ByteArrayOutputStream()
//       bos.write(result.array())
//       bos
//    }

//    def decode(dataSize: Int, data: ByteArrayInputStream): Array[T#A] = {
//        val intData = new Array[Int](dataSize)
//        for (i <- intData.indices) {
//            intData(i) = data.read()
//        }

//        iic.uncompress(intData)
//    }
// }
