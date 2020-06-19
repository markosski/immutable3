package immutabledb.operator

import immutabledb._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.HashMap

object OperatorAliasses {
    type AggMap = HashMap[String, Aggregator[_, _]]
    type AggMapTuple = (String, AggMap)
}

import OperatorAliasses._

trait Operator[A] extends LazyLogging {
    def iterator: Iterator[A]
}

trait ColumnVectorOperator extends Operator[ColumnVectorBatch] {
    def iterator: Iterator[ColumnVectorBatch]
}

trait ProjectionAggregateOperator extends Operator[AggMapTuple] {
    def iterator: Iterator[AggMapTuple]
}

trait ProjectionOperator extends Operator[Row] {
    def iterator: Iterator[Row]
}