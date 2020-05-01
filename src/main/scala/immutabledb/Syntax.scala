package immutabledb.syntax

trait ColExpr {
    val alias: Option[String]
}

object ColExprFunc extends Enumeration {
  type FuncName = Value
  val ADD, SUBSTR, MULT = Value
}

case class SimpleExpr(colName: String, alias: Option[String] = None) extends ColExpr
case class PredicateExpr(colName: String, op: ColExprFunc.FuncName, expr: ColExpr, alias: Option[String] = None) extends ColExpr

trait Select {}
case class SimpleSelect(cols: List[ColExpr])

trait Aggregate {}

case class Query(tableName: String, colExpr: List[ColExpr], aggregate: Aggregate)