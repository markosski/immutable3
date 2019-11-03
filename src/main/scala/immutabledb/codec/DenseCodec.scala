package immutabledb.codec

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer

import immutabledb.DataType

import scala.reflect.ClassTag

/**
  * Created by marcin1 on 2/21/17.
  */
class DenseCodec[A, T <: DataType[A]](val dtype: T)(implicit tag: ClassTag[A]) extends Codec[A] {
    def encode(values: Array[String]) = {
        val result = new ByteArrayOutputStream()

        for (i <- values.indices) {
            result.write(dtype.valueToBytes(dtype.stringToValue(values(i))))
        }

        result
    }

    def decode(dataSize: Int, data: ByteArrayInputStream): Array[A] = {
        val segment = new Array[A](dataSize)
        val chunk = new Array[Byte](dtype.size)

        var i = 0
        while (data.read(chunk) != -1) {
            segment(i) = dtype.bytesToValue(chunk)
            i += 1
        }
        segment
    }
}
