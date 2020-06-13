package immutabledb.codec

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer
import scala.collection.mutable.Buffer
import immutabledb.DataType
import immutabledb._

import scala.reflect.ClassTag

/**
  * Created by marcin1 on 2/21/17.
  */

trait DenseCodec {
    val dtype: DataType

    def encode(values: Array[Byte]) = {
        val result = new ByteArrayOutputStream()
        result.write(values)
        result
    }

    def encode(values: Array[String]) = {
        val result = new ByteArrayOutputStream()

        for (i <- values.indices) {
            result.write(dtype.valueToBytes(dtype.stringToValue(values(i))))
        }
        result
    }
}

object DenseCodecInt extends Codec[Int] with DenseCodec {
    val dtype = IntType

    def decode(data: ByteArrayInputStream): Array[Int] = {
        val segment = Buffer[Int]()
        val chunk = new Array[Byte](dtype.size)

        while (data.read(chunk) != -1) {
            segment += dtype.bytesToValue(chunk)
        }
        segment.toArray
    }
}

object DenseCodecTinyInt extends Codec[Byte] with DenseCodec {
    val dtype = TinyIntType

    def decode(data: ByteArrayInputStream): Array[Byte] = {
        val segment = Buffer[Byte]()
        val chunk = new Array[Byte](dtype.size)

        while (data.read(chunk) != -1) {
            segment += dtype.bytesToValue(chunk)
        }
        segment.toArray
    }
}

class DenseCodecString(size: Int) extends Codec[String] with DenseCodec {
    val dtype = StringType(size)

    def decode(data: ByteArrayInputStream): Array[String] = {
        val segment = Buffer[String]()
        val chunk = new Array[Byte](dtype.size)

        while (data.read(chunk) != -1) {
            segment += dtype.bytesToValue(chunk)
        }
        segment.toArray
    }
}

// class DenseCodec[T <: DataType](val dtype: T)(implicit tag: ClassTag[T#A]) extends Codec[T] {
//     def encode(values: Array[Byte]) = {
//         val result = new ByteArrayOutputStream()
//         result.write(values)
//         result
//     }

//     def encode(values: Array[String]) = {
//         val result = new ByteArrayOutputStream()

//         for (i <- values.indices) {
//             result.write(dtype.valueToBytes(dtype.stringToValue(values(i))))
//         }
//         result
//     }

//     def decode(dataSize: Int, data: ByteArrayInputStream): Array[T#A] = {
//         val segment = new Array[T#A](dataSize)
//         val chunk = new Array[Byte](dtype.size)

//         var i = 0
//         while (data.read(chunk) != -1) {
//             segment(i) = dtype.bytesToValue(chunk)
//             i += 1
//         }
//         segment
//     }
// }
