package immutabledb.codec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import immutabledb.DataType

trait Codec[A] {
  val dtype: DataType[A]
  def encode(data: Array[String]): ByteArrayOutputStream
  def decode(dataSize: Int, data: ByteArrayInputStream): Array[A]
}
