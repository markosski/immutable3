package immutabledb

import immutabledb.storage.SegmentManager
import immutabledb.operator._
import scala.collection.mutable.MutableList

// object Pipeline {
//     def mkPipeline(sm: SegmentManager, tableName: String, cols: List[String]): Pipeline = {
//         val table = sm.getTable(tableName)
//         val columns = table.columns.filter( c => cols.contains(c.name) )

//         new Pipeline(sm, tableName, columns)
//     }
// }

trait SelectCondition 
case class Match(values: List[String]) extends SelectCondition
case class NotMatch(values: List[String]) extends SelectCondition
case class GT(gt: Double) extends SelectCondition
case class LT(lt: Double) extends SelectCondition
case object NoOp extends SelectCondition

trait SelectADT
case class And(op1: Select, op2: Select) extends SelectADT
case class Or(op1: Select, op2: Select) extends SelectADT
case class Select(col: String, cond: SelectCondition) extends SelectADT

trait ProjectADT
case class Project(cols: List[String], limit: Int = 0) extends ProjectADT

/*
And(
    Select("age", GT(30)),
    Or(
        Select("state", Match(List("VA"))),
        Select("name", Match(List("Marcin")))
    )
)
*/
case class Query(
    table: String, 
    select: SelectADT,
    project: ProjectADT
)

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
case class PLeaf[T <: SelectionOperator](op: T => T) extends PTree
case class PNode(n1: PTree, n2: PTree, op: PipelineOp.Value) extends PTree

case class Pipeline(
    table: Table,
    columns: List[Column],
    scanOp: (List[Column], Int) => SelectionOperator,
    selectOps: PTree,
    projectOp: SelectionOperator => ProjectionOperator
) {
    override def toString = {
        s"""
        |Pipeline:
        |   table: ${this.table}
        |   columns: ${this.columns}
        |   scanOp: ${this.scanOp}
        |   selectOps: ${this.selectOps}
        |   projectOp: ${this.projectOp}
        """.stripMargin
    }
}