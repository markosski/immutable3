package immutabledb.sql

import scala.util.parsing.combinator._
import immutabledb._

// https://scala-lms.github.io/tutorials/query.html
object SQLParser extends JavaTokenParsers {
    def parseAll(input: String): Query = parseAll(query, input.toLowerCase) match {
        case Success(res, _)  => res
        case res => throw new Exception(res.toString)
    }

    def query: Parser[Query] = queryProjectAgg | queryProjectAggNoGroup | queryProject

    def queryProjectAgg: Parser[Query] = {
        selectProjectAgg ~ fromTable ~ where ~ groupBy ^^ {
            case p ~ t ~ w ~ g => Query(t, w, p.copy(groupBy = g))
        }
    }

    def queryProjectAggNoGroup: Parser[Query] = {
        selectProjectAgg ~ fromTable ~ where ^^ {
            case p ~ t ~ w => Query(t, w, p)
        }
    }

    def queryProject: Parser[Query] = {
        selectProject ~ fromTable ~ where ~ limit ^^ {
            case p ~ t ~ w ~ l => Query(t, w, p.copy(limit = l.getOrElse(0)))
        }
    }

    def groupBy: Parser[List[String]] = {
        "group" ~> "by" ~> fieldIdList1
    }

    def limit: Parser[Option[Int]] = opt("limit" ~> """[\d]+""".r ^^ { x => x.toInt })

    def fromTable: Parser[String] = {
        "from" ~> fieldIdent
    }

    def selectProjectAgg: Parser[ProjectAgg] = {
        "select" ~> projectAggsP
    }

    def selectProject: Parser[Project] = {
        "select" ~> projectP
    }

    def where: Parser[SelectADT] = {
        opt("where" ~> filter) ^^ {
            case Some(value) => value
            case None => NoSelect
        }
    }

    def filter: Parser[SelectADT] = {
        filterAnd | filterOr | filterEQ | filterGT | filterLT
    }

    def filterAnd: Parser[SelectADT] = {
        "(" ~> repsep(filter, "and") <~ ")" ^^ {
            case xs => xs.tail.foldLeft(xs.head) { (a, b) =>  And(a, b) }
        }
    }

    def filterOr: Parser[SelectADT] = {
        "(" ~> repsep(filter, "or") <~ ")" ^^ {
            case xs => xs.tail.foldLeft(xs.head) { (a, b) =>  Or(a, b) }
        }
    }

    def filterEQ: Parser[SelectADT] = {
        fieldIdent ~ "=" ~ value ^^ {
            case f ~ _ ~ v => Select(f, EQ(v.toDouble))
        }
    }

    def filterGT: Parser[SelectADT] = {
        fieldIdent ~ ">" ~ value ^^ {
            case f ~ _ ~ v => Select(f, GT(v.toDouble))
        }
    }

    def filterLT: Parser[SelectADT] = {
        fieldIdent ~ "<" ~ value ^^ {
            case f ~ _ ~ v => Select(f, LT(v.toDouble))
        }
    }

    def projectP: Parser[Project] = 
        repsep(fieldIdent, ",") ^^ (fs => Project(fs) )

    def projectAggsP: Parser[ProjectAgg] = rep1sep(aggP, ",") ^^ { aggs => ProjectAgg(aggs)}

    def aggP: Parser[Aggregate] = aggSumP | aggMinP | aggMaxP | aggCountP

    def aggSumP: Parser[Aggregate] = {
        "sum" ~> "(" ~> fieldIdent <~ ")" ^^ { case name => Sum(name) }
    }

    def aggMinP: Parser[Aggregate] = {
        "min" ~> "(" ~> fieldIdent <~ ")" ^^ { case name => Min(name) }
    }

    def aggMaxP: Parser[Aggregate] = {
        "max" ~> "(" ~> fieldIdent <~ ")" ^^ { case name => Max(name) }
    }

    def aggCountP: Parser[Aggregate] = {
        "count" ~> "(" ~> fieldIdent <~ ")" ^^ { case name => Count(name) }
    }

    def fieldIdent: Parser[String] = """[\w\#]+""".r

    def value: Parser[String] = """[\w0-9\#]+""".r

    def fieldIdList: Parser[List[String]] = 
        repsep(fieldIdent,",") ^^ (fs => List(fs:_*))

    def fieldIdList1: Parser[List[String]] = 
        rep1sep(fieldIdent,",") ^^ (fs => List(fs:_*))
}