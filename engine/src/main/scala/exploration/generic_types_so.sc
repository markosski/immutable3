import scala.collection.mutable
import scala.reflect.ClassTag

trait DataType {
    type DType
}

object IntType extends DataType {
    type DType = Int
}

trait Codec {
    val dtype: DataType
}

class DenseCodec(val dtype: DataType) extends Codec {
    type DType = dtype.DType

    def get[DType : ClassTag](size: Int) = new Array[DType](size)
}

val dense = new DenseCodec(IntType)

dense.get[IntType.DType](4)
