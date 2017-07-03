package immutabledb.codec

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer

import immutabledb.DataType

import scala.reflect.ClassTag

/**
  * Created by marcin1 on 2/21/17.
  */
class DenseCodec[T <: DataType](val dtype: T)(implicit tag: ClassTag[T#A]) extends Codec[T] {
    /**
      * @param values
      * @return
      */
    def encode(values: Array[String]) = {
        val result = new ByteArrayOutputStream()

        for (i <- values.indices) {
            result.write(dtype.valueToBytes(dtype.stringToValue(values(i))))
        }

        result
    }

    def decode(dataSize: Int, data: ByteArrayInputStream): Array[T#A] = {
        val segment = new Array[T#A](dataSize)
        val chunk = new Array[Byte](dtype.size)

        var i = 0
        while (data.read(chunk) != -1) {
            segment(i) = dtype.bytesToValue(chunk)
            i += 1
        }
        segment
    }
}
