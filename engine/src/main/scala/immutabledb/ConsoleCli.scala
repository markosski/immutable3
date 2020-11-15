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
import java.time.Instant

object ConsoleCliParser {
    case class Config(
        query: String = "", 
        dataPath: List[File] = Nil,
        cpuCount: Int = Runtime.getRuntime().availableProcessors() 
        )

    val builder = OParser.builder[Config]
    val parser = {
        import builder._
        OParser.sequence(
            programName("ImmutableDB Console SQL Runner"),
            head("*******************", "v0.1"),
            opt[File]('d', "data-dir")
                .required()
                .valueName("<dir>")
                .action((v, c) => c.copy(dataPath = v :: Nil))
                .text("path to data directory"),
            opt[Int]("cpu-count")
                .valueName("<num>")
                .action((v, c) => c.copy(cpuCount = v))
                .text("number of threads to use")
        )
    }

    def parse(args: List[String]): Option[Config] = {
        OParser.parse(parser, args, Config())
    }
}

object ConsoleCli extends LazyLogging {
    def run(engine: Engine): Unit = {
        Try {
            val inputQuery = scala.io.StdIn.readLine("\nsql> ")

            val query: Query = SQLParser.parseAll(inputQuery)
            logger.info(s"Query: $query")

            val startTime = System.currentTimeMillis()
            val res = engine.execute(query)
            val finishTime = System.currentTimeMillis() - startTime
            res.fold(
                err => println(err), 
                it => while (it.hasNext) { println(it.next) }
            )
            println(s"Query executed in: ${finishTime} millis")
        } match {
            case Failure(err) => println(err);
            case _ => ()
        }

        run(engine)
    }

    def main(args: Array[String]): Unit = {
        logger.debug(s"Args: $args")

        val config = ConsoleCliParser.parse(args.toList).fold {
            println(s"Error parsing arguments: ${args.toList}")
            sys.exit()
        }(identity)
        logger.info(s"CLI Config: $config")

        val sm = new SegmentManager(config.dataPath.head.getAbsolutePath())
        val th = java.util.concurrent.Executors.newFixedThreadPool(config.cpuCount)
        val engine = new Engine(sm, th)

        run(engine) 
    }
}