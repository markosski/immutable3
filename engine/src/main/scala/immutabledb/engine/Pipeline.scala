package immutabledb.engine

import immutabledb._
import immutabledb.storage.SegmentManager
import immutabledb.operator._
import scala.collection.mutable.MutableList

object PipelineOp extends Enumeration {
    val AND, OR = Value
}

/*
PNode(
    PNode(
        PLeaf(),
        PLeaf(),
        OR
    ),
    PLeaf(),
    AND
)
*/
trait PTree
case class PLeaf[T <: ColumnVectorOperator](op: T => T) extends PTree
case class PNode(n1: PTree, n2: PTree, op: PipelineOp.Value) extends PTree

case class Pipeline[R](
                       table: Table,
                       usedColumns: List[Column],
                       scanOp: (List[Column], Int) => ColumnVectorOperator,
                       selectOps: PTree,
                       projectOp: ColumnVectorOperator => Operator[R]
) {
    override def toString = {
        s"""
        |Pipeline:
        |   table: ${this.table}
        |   usedColumns: ${this.usedColumns}
        |   scanOp: ${this.scanOp}
        |   selectOps: ${this.selectOps}
        |   projectOp: ${this.projectOp}
        """.stripMargin
    }
}