package immutabledb

/**
  * Created by marcin1 on 2/21/17.
  */

// object DType extends Enumeration {
//   type DType = Value
//   val INT, TINY_INT, STRING = Value
// }

trait DataType {
    type A

    val size: Int
    val nullRepr: A
    def stringToValue(s: String): A
    def valueToBytes(value: A): Array[Byte]
    def bytesToValue(bytes: Array[Byte]): A
    def stringToBytes(s: String): Array[Byte] = valueToBytes(stringToValue(s))
}

trait NumericDataType extends DataType {
    implicit val numOps: Numeric[A]
    val minVal: A
    val maxVal: A
}

case object IntType extends NumericDataType {
    type A = Int

    val numOps = implicitly[Numeric[Int]]
    val size = 4
    val minVal = Int.MinValue + 1
    val maxVal = Int.MaxValue
    val nullRepr: Int = Int.MinValue
    def stringToValue(s: String): Int = s.toInt
    def valueToBytes(value: Int): Array[Byte] = {
        val bytes = new Array[Byte](4)
        bytes(3) = ((value >> 24) & 0xFF).toByte
        bytes(2) = ((value >> 16) & 0xFF).toByte
        bytes(1) = ((value >> 8) & 0xFF).toByte
        bytes(0) = (value & 0xFF).toByte
        bytes
    }
    def bytesToValue(bytes: Array[Byte]): Int = Conversions.bytesToInt(bytes)
}

case object TinyIntType extends NumericDataType {
    type A = Byte

    val numOps = implicitly[Numeric[Byte]]
    val size = 1
    val minVal = Byte.MinValue + 1
    val maxVal = Byte.MaxValue
    val nullRepr: Byte = Byte.MinValue
    def stringToValue(s: String): Byte = s.toByte
    def valueToBytes(value: Byte): Array[Byte] = Array(value)
    def bytesToValue(bytes: Array[Byte]): Byte = bytes(0)
}

case class StringType(val size: Int) extends DataType {
    type A = String

    val nullRepr: String = "\\N"
    def stringToValue(s: String): String = s
    def valueToBytes(value: String): Array[Byte] = value.getBytes()
    def bytesToValue(bytes: Array[Byte]): String = new String(bytes)
}

//trait DType {
//    type DTYPE
//    val size: Int
//    val nullRepr: DTYPE
//    def bytesToValue(bytes: Array[Byte]): DTYPE
//    def stringToBytes(s: String): Array[Byte]
//}
//
//
//case object ShortType extends DType {
//    type DTYPE = Short
//
//    val size = 2
//    val nullRepr: Short = Short.MinValue
//    def bytesToValue(bytes: Array[Byte]) = ???
//    def stringToBytes(s: String): Array[Byte] = ???
//}
//
//case object IntType extends DType {
//    type DTYPE = Int
//    val size = 4
//    val nullRepr: Int = Int.MinValue
//    def bytesToValue(bytes: Array[Byte]) = ???
//    def stringToBytes(s: String): Array[Byte] = ???
//}
//
//case object LongType extends DType {
//    type DTYPE = Long
//    val size = 8
//    val nullRepr: Long = Long.MinValue
//    def bytesToValue(bytes: Array[Byte]) = ???
//    def stringToBytes(s: String): Array[Byte] = ???
//}
//
//case class StringType(size: Int) extends DType {
//    type DTYPE = String
//    val nullRepr: String = "\\N"
//    def bytesToValue(bytes: Array[Byte]) = ???
//    def stringToBytes(s: String): Array[Byte] = ???
//}
