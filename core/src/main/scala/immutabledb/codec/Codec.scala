package immutabledb.codec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import immutabledb._

/**
  * Created by marcin on 12/20/16.
  *
  * Only purpose of the codec is to encode or decode data. Storage of encoded data is handled by ...
  */

trait Codec[T] {
    val dtype: DataType

    def encode(data: Array[Byte]): ByteArrayOutputStream
    def encode(data: Array[String]): ByteArrayOutputStream
    def decode(data: ByteArrayInputStream): Array[T]
}

object CodecType extends Enumeration {
  type Codec = Value
  val PFOR_INT, DENSE_INT, DENSE_TINYINT, DENSE_STRING = Value
}