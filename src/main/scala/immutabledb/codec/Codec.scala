package immutabledb.codec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import immutabledb.DataType

/**
  * Created by marcin on 12/20/16.
  *
  * Only purpose of the code is to encode or decde data. Storage of encoded data is handles by ...
  */

trait Codec[T <: DataType] {
    val dtype: T

    def encode(data: Array[String]): ByteArrayOutputStream
    def decode(dataSize: Int, data: ByteArrayInputStream): Array[T#A]
}
