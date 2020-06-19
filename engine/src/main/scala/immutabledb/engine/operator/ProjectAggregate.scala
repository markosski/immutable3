package immutabledb.operator

import immutabledb._

import scala.collection.mutable
import scala.collection.mutable.{Buffer, HashMap}
import java.math.BigInteger
import OperatorAliasses.{AggMap, AggMapTuple}


trait Aggregator[T, R] extends Product {
    val col: String
    val alias: String
    def add(v: T)
    def get: R
    def combine(agg: Aggregator[T, R]): Aggregator[T, R]
    def repr: String
    def make: Aggregator[T, R]
}

case class CountAggr(col: String, alias: String) extends Aggregator[Any, Long] {
    private var counter = 0L
    def add(v: Any) = counter += 1
    def set(newCounter: Long) = {
        counter = newCounter
    }
    def combine(agg: Aggregator[Any, Long]): Aggregator[Any, Long] = {
       counter += agg.get 
       this
    }
    def get: Long = counter
    def repr = get.toString
    def make = CountAggr(col, alias)
}

case class MaxDoubleAggr(col: String, alias: String) extends Aggregator[Double, Double] {
    private var max = Double.MinValue
    def add(v: Double) = if (v > max) max = v
    def get: Double = max
    def combine(agg: Aggregator[Double, Double]): Aggregator[Double, Double] = {
        add(agg.get)
        this
    }
    def repr = get.toString
    def make = MaxDoubleAggr(col, alias)
}

case class MinDoubleAggr(col: String, alias: String) extends Aggregator[Double, Double] {
    private var min = Double.MaxValue
    def add(v: Double) = if (v < min) min = v
    def get: Double = min
    def combine(agg: Aggregator[Double, Double]): Aggregator[Double, Double] = {
        add(agg.get)
        this
    }
    def repr = get.toString
    def make = MinDoubleAggr(col, alias)
}

case class AvgDoubleAggr(col: String, alias: String) extends Aggregator[Double, (BigDecimal, Long)] {
    private var sum = BigDecimal(0.0)
    private var counter = 0L
    def add(v: Double) = {
        sum += v
        counter += 1
    }
    def combine(agg: Aggregator[Double, (BigDecimal, Long)]): Aggregator[Double, (BigDecimal, Long)] = {
        sum += agg.get._1
        counter += agg.get._2
        this
    }
    def get = (sum, counter)
    def repr = (sum / counter).toString
    def make = AvgDoubleAggr(col, alias)
}

case class MaxStringAggr(col: String, alias: String) extends Aggregator[String, String] {
    private var max = ""
    def add(v: String) = 
        if (max == "") max = v
        else if (v > max) max = v
    def get: String = max
    def combine(agg: Aggregator[String, String]): Aggregator[String, String] = {
        add(agg.get)
        this
    }
    def repr = get.toString
    def make = MaxStringAggr(col, alias)
}

case class MinStringAggr(col: String, alias: String) extends Aggregator[String, String] {
    private var min = ""
    def add(v: String) = 
        if (min == "") min = v 
        else if (v < min) min = v
    def get: String = min
    def combine(agg: Aggregator[String, String]): Aggregator[String, String] = {
        add(agg.get)
        this
    }
    def repr = get.toString
    def make = MinStringAggr(col, alias)
}


object ProjectAggOp {
    def make(aggs: List[Aggregator[_, _]], groupBy: List[String]) = new Function1[ColumnVectorOperator, ProjectAggOp] {
        override def toString = s"aggs = $aggs, groupBy = $groupBy"
        def apply(op: ColumnVectorOperator) = {
            new ProjectAggOp(aggs, op, groupBy)
        }
    } 
}

class ProjectAggOp(aggs: List[Aggregator[_, _]], op: ColumnVectorOperator, groupBy: List[String]) extends ProjectionAggregateOperator {
    override def toString = s"aggs = $aggs, op = $op, groupBy = $groupBy"

    def iterator = new ProjectAggIterator

    class ProjectAggIterator extends Iterator[AggMapTuple] {
        private val opIter = op.iterator
        private val cols: List[String] = aggs.map(_.col)
        private val aliases: Map[String, String] = aggs.map(x => (x.alias, x.col)).toMap
        private val resultMap = mutable.LinkedHashMap[String, AggMap]()
        private val aggsMap: HashMap[String, Aggregator[_, _]] = HashMap.apply(aggs.map(x => (x.alias, x)):_*)
        private var currVecBatch: ColumnVectorBatch = null

        // Need to find cols in proper order
        private lazy val aggsCols: Map[String, Int] = currVecBatch.columns.toList.zipWithIndex
            .filter( p => cols.contains(p._1.name) )
            .map( p => (p._1.name, p._2) )
            .toMap

        private lazy val groupCols: Map[String, Int] = currVecBatch.columns.toList.zipWithIndex
            .filter( p => groupBy.contains(p._1.name) )
            .map( p => (p._1.name, p._2) )
            .toMap

        private lazy val groupColsValues = groupCols.values.toList

        // Create group hashmap
        runAggs()

        private val resultMapIter = resultMap.iterator

        private def getResultMapKey(xs: List[Any]) = xs.mkString("_")

        private def getNewAggs(aggs: AggMap): AggMap = {
            val newAggs = HashMap[String, Aggregator[_, _]]()
            aggs.foreach( keyVal => newAggs.put(keyVal._1, keyVal._2.make) )
            newAggs
        }

        private def runAggs(): Unit = while (opIter.hasNext) {
            currVecBatch = opIter.next
            val currVecSize = currVecBatch.size

            // skip over non selected
            for (currVecBatchPos <- currVecBatch.selected) {
                val groupKey: String = getResultMapKey(
                    groupColsValues.map { idx =>
                        ColumnVectorBatch.getValue[Any](currVecBatch, idx, currVecBatchPos)
                    }
                )
                logger.debug(s"resultMap group key: $groupKey")

                for (alias <- aliases.keys) {
                    val colName = aliases(alias)
                    val cIdx = aggsCols(colName)
                    val col = cols(cIdx)
                    val currVec = currVecBatch.columnVectors(cIdx)

                    currVec match {
                        case IntColumnVector(_) => {
                            val value = currVec.data(currVecBatchPos).asInstanceOf[Int]
                            resultMap.getOrElseUpdate(groupKey, getNewAggs(aggsMap))

                            logger.debug(s"resultMap state: $resultMap")

                            resultMap(groupKey)(alias) match {
                                case aggr: CountAggr     => aggr.add(value)
                                case aggr: MaxDoubleAggr => aggr.add(value.toDouble)
                                case aggr: MinDoubleAggr => aggr.add(value.toDouble)
                                case aggr: AvgDoubleAggr => aggr.add(value.toDouble)
                                case _ => throw new Exception("bad aggregator for this data type")
                            }
                        }
                        case TinyIntColumnVector(_) => {
                            val value = currVec.data(currVecBatchPos).asInstanceOf[Byte]
                            resultMap.getOrElseUpdate(groupKey, getNewAggs(aggsMap))

                            logger.debug(s"resultMap state: $resultMap")

                            resultMap(groupKey)(alias) match {
                                case aggr: CountAggr     => aggr.add(value)
                                case aggr: MaxDoubleAggr => aggr.add(value.toDouble)
                                case aggr: MinDoubleAggr => aggr.add(value.toDouble)
                                case aggr: AvgDoubleAggr => aggr.add(value.toDouble)
                                case _ => throw new Exception("bad aggregator for this data type")
                            }
                        }
                        case StringColumnVector(_) => {
                            val value = currVec.data(currVecBatchPos).asInstanceOf[String]
                            resultMap.getOrElseUpdate(groupKey, getNewAggs(aggsMap))

                            logger.debug(s"resultMap state: $resultMap")

                            resultMap(groupKey)(alias) match {
                                case aggr: CountAggr     => aggr.add(value)
                                case aggr: MaxStringAggr => aggr.add(value)
                                case _ => throw new Exception("bad aggregator for this data type")
                            }
                        }
                        case _ => throw new Exception(s"Cannot perform aggregations on vector type $currVec")
                    }
                }
            }

            logger.debug(s"resultMap: $resultMap")
        }

        def next: AggMapTuple = resultMapIter.next

        def hasNext = resultMapIter.hasNext
    }
}