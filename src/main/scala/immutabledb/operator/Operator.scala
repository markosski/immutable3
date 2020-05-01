package immutabledb.operator

import immutabledb._
import com.typesafe.scalalogging.LazyLogging

trait Operator[A] extends LazyLogging {
    def iterator: Iterator[A]
}

trait SelectionOperator extends Operator[ColumnVectorBatch] {
    def iterator: Iterator[ColumnVectorBatch]
}

trait ProjectionOperator extends Operator[Seq[_]] {
    def iterator: Iterator[Seq[_]]
}