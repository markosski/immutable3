import java.io.Closeable
import java.nio.ByteBuffer
import scala.reflect.runtime.universe._

class ClassA[T : TypeTag](l: List[T]) {
    def writeData: List[T] = {

        val segment = typeOf[T] match {
            case t if t =:= typeOf[Int] => List[T]()
            case t if t =:= typeOf[String] => List[String]()
            case _ => throw new Exception("Not supported segment type.")
        }

        segment
    }
}

val c = new ClassA(List("A"))
c.writeData