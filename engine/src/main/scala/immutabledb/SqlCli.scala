package immutabledb

import java.util.concurrent.ExecutorService
import immutabledb._
import immutabledb.engine._
import immutabledb.storage._
import immutabledb.operator._
import immutabledb.sql.SQLParser
import com.typesafe.scalalogging.LazyLogging
import java.io.File
import scala.util.{Try, Success, Failure}
import scopt.OParser


object SqlCliParser {
    case class Config(
        query: String = "", 
        dataPath: List[File] = Nil,
        )

    val builder = OParser.builder[Config]
    val parser = {
        import builder._
        OParser.sequence(
            programName("ImmutableDB SQL Runner"),
            head("*******************", "v0.1"),
            opt[String]('q', "query")
                .required()
                .valueName("<string>")
                .action((v, cfg) => cfg.copy(query = v))
                .text("SQL query"),
            opt[File]('d', "data-dir")
                .required()
                .valueName("<dir>")
                .action((v, c) => c.copy(dataPath = v :: Nil))
                .text("path to data directory")
        )
    }

    def parse(args: List[String]): Option[Config] = {
        OParser.parse(parser, args, Config())
    }
}

object SqlCli extends LazyLogging {
    def main(args: Array[String]): Unit = {
        logger.debug(s"Args: $args")

        val config = SqlCliParser.parse(args.toList).fold {
            println(s"Error parsing arguments: ${args.toList}")
            sys.exit()
        }(identity)
        logger.info(s"CLI Config: $config")

        val query: Query = SQLParser.parseAll(config.query)
        logger.info(s"Query: $query")

        // implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

        val cpus = Runtime.getRuntime().availableProcessors()
        val th = java.util.concurrent.Executors.newFixedThreadPool(cpus)
        val sm = new SegmentManager(config.dataPath.head.getAbsolutePath())
        val table = sm.getTable(query.table)
        val engine = new Engine(sm, th)

        val res = engine.execute(query)
        res.fold(
            err => println(err), 
            it => while (it.hasNext) { println(it.next) }
        )
        System.exit(0)
    }
}