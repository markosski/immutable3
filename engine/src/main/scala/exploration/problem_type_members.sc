import scala.reflect.{ClassTag, classTag}

trait DataType {
    type A
    implicit def tag: ClassTag[A]

    def convert(value: String): A
    def convertToString(value: A): String
}

case object IntType extends DataType {
    type A = Int
    implicit val tag = classTag[A]

    def convert(value: String) = value.toInt
    def convertToString(value: Int) = value.toString
}

trait Codec[T <: DataType] {
    val dtype: T

    def encode(data: Array[String])(implicit tag: ClassTag[dtype.A]): Array[dtype.A]
    def decode(data: Array[dtype.A]): Array[String]
}

class CodecImp[T <: DataType](val dtype: T) extends Codec[T] {
    def encode(data: Array[String])(implicit tag: ClassTag[dtype.A]) = {
        Array[dtype.A](dtype.convert(data(0)))
    }

    def decode(data: Array[dtype.A]) = {
        Array[String](dtype.convertToString(data(0)))
    }
}

//
//class SegmentReader[T <: DataType](path: String, codec: Codec[T], segmentSize: Int) extends Iterable[Array[T#A]] {
//    def iterator = new SegmentIterator
//
//    class SegmentIterator extends Iterator[Array[T#A]] {
//        def next: Array[T#A] = ???
//        def hasNext: Boolean = ???
//    }
//}
//
val cod = new CodecImp(IntType)
val encoded = cod.encode(Array("1", "2", "3")) // Array[IntType.A]
val decoded = cod.decode(encoded) // Array[String]


//class Foo[T](dtype: DataType {type A = T}) {
//    def gimme(xs: List[String]): List[T] = {
//        xs.map(x => dtype.convert(x))
//    }
//
//    def here(xs: List[T]): List[String] = {
//        xs.map(x => dtype.convertToString(x))
//    }
//}
//
//val foo = new Foo(IntType)
//val data = foo.gimme(List("1", "2", "3"))
//foo.here(data)
