package immutabledb

trait SelectCondition 
case class Match(values: List[String]) extends SelectCondition
case class NotMatch(values: List[String]) extends SelectCondition
case class EQ(eq: Double) extends SelectCondition
case class GT(gt: Double) extends SelectCondition
case class LT(lt: Double) extends SelectCondition
case object NoOp extends SelectCondition

trait SelectADT
case class And(op1: Select, op2: Select) extends SelectADT
case class Or(op1: Select, op2: Select) extends SelectADT
case class Select(col: String, cond: SelectCondition) extends SelectADT
case object NoSelect extends SelectADT

trait Aggregate {
  val col: String
  val alias: Option[String]
}
case class Sum(col: String, alias: Option[String] = None) extends Aggregate
case class Avg(col: String, alias: Option[String] = None) extends Aggregate
case class Min(col: String, alias: Option[String] = None) extends Aggregate
case class Max(col: String, alias: Option[String] = None) extends Aggregate
case class Count(col: String, alias: Option[String] = None) extends Aggregate

// add sort
trait ProjectADT
case class Project(cols: List[String], limit: Int = 0) extends ProjectADT
case class ProjectAgg(aggs: List[Aggregate], groupBy: List[String] = Nil) extends ProjectADT

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
