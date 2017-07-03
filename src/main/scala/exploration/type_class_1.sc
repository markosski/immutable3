//import java.nio.ByteBuffer
//
//import codec.Codec
//
//trait Segment[T] {
//    val data: ByteBuffer
//}
//
//trait SegmentChecker[T] {
//    def checkValue(value: T): Boolean
//}
//
//case class NonNumericSegment[T](data: ByteBuffer) extends Segment[T]
//
//case class NumericSegment[T : Numeric](data: ByteBuffer, min: T, max: T) extends Segment[T] with SegmentChecker[T] {
//    def checkValue(value: T): Boolean = {
//        val num = implicitly[Numeric[T]]
//
//        if (num.gteq(value, min) && num.lteq(value, max)) true else false
//    }
//}
//
//case class Column[T](codec: Codec[T], name: String, table: String)
//
//class SegmentWriter[T](column: Column[T], segmentSize: Int) {
//    def writeData(data: Array[String]): Unit = {
//
//    }
//}

case class Dep[T](value: T)

case class NumericDep[T](override val value: T, min: T, max: T) extends Dep[T](value)

class DefaultConrete[T](dep: Dep[T]) {
    def doSomething(a: List[T]): List[T] = {
        a
    }
}

class NumericConcrete[T : Numeric](dep: Dep[T]) {
   def doSomething(a: List[T]): List[T] = {
       var num = implicitly[Numeric[T]]

       var min = a(0)
       for (current <- a) {
           if (num.lt(current, min)) min == current
       }

       List(min)
   }
}


def create[T](dep: Dep[T])(implicit num: Numeric[T]) = {
    dep match {
        case x: NumericDep[_] => new NumericConcrete(dep)
        case _ => new DefaultConrete(dep)
    }
}