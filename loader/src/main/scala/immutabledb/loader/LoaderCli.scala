package immutabledb.loader

import immutabledb._
import immutabledb.codec._
import immutabledb.storage._
import java.io.File
import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import scala.util.{Try, Success, Failure}
import scopt.OParser

/*
--col id,DENSE_INT --col state,DENSE_STRING,{}
*/

object LoaderCliParser {
    case class ArgCol(name: String, codec: String, options: Map[String, String])
    case class Config(
        cols: Vector[ArgCol] = Vector.empty, 
        table: String = "", 
        dataPath: List[File] = Nil, 
        inputPath: List[File] = Nil,
        blockSize: Int = DevEnv.config.blockSize,
        segSize: Int = DevEnv.config.segmentSize
        )

    val builder = OParser.builder[Config]
    val parser = {
        import builder._
        OParser.sequence(
            programName("ImmutableDB Loader"),
            head("*******************", "v0.1"),
            opt[String]('t', "table-name")
                .required()
                .valueName("<string>")
                .action((v, cfg) => cfg.copy(table = v))
                .text("table name"),
            opt[Seq[String]]('c', "cols")
                .required()
                .valueName("<COL_DEF>,<COL_DEF>,...")
                .action((v, cfg) => cfg.copy(cols = v.map(parseCol).toVector))
                .text(
                    "column definition, where COL_DEF = COL_NAME:COL_CODEC[:OPTIONS], OPTIONS = KEY=VALUE[;KEY=VALUE;...], " +
                    "example - id:DENSE_INT,state:DENSE_STRING:size=2,age:DENSE_TINYINT"),
            opt[File]('d', "data-dir")
                .required()
                .valueName("<dir>")
                .action((v, c) => c.copy(dataPath = v :: Nil))
                .text("path to data directory"),
            opt[File]('i', "input-csv")
                .required()
                .valueName("<file>")
                .action((v, c) => c.copy(inputPath = v :: Nil))
                .text("path to CSV file"),
            opt[Int]("block-size")
                .valueName("<int>")
                .action((v, c) => c.copy(blockSize = v))
                .text("number of records per block"),
            opt[Int]("segment-size")
                .valueName("<int>")
                .action((v, c) => c.copy(blockSize = v))
                .text("number of blocks per segment")
        )
    }

    def parseColOptions(colOptionsArg: String): Map[String, String] = {
        colOptionsArg.split(";").map(x => x.split("=")).map(xs => (xs.head, xs.last)).toMap
    }

    def parseCol(colArg: String): ArgCol = {
        val parts = colArg.split(":")

        val options = if (parts.length == 3) {
            parseColOptions(parts(2))
        } else {
            Map[String, String]()
        }

        ArgCol(parts(0), parts(1), options)
    }

    def parse(args: List[String]): Option[Config] = {
        OParser.parse(parser, args, Config())
    }
}

object LoaderCli extends LazyLogging {
    def main(args: Array[String]) = {
        logger.debug(s"Args: $args")

        val config = LoaderCliParser.parse(args.toList).fold {
            println(s"Error parsing arguments: ${args.toList}")
            sys.exit()
        }(identity)
        
        logger.info(s"CLI Config: $config")

        val csvFilePath = config.inputPath.head
        val outDir = config.dataPath.head

        val bufferedSource = io.Source.fromFile(csvFilePath)

        val lines = bufferedSource.getLines()
        val first = lines.next().split(",").toList

        val cols = config.cols.toList.map { col => col.codec match {
                case "DENSE_INT" => Column.make(col.name, CodecType.DENSE_INT)
                case "DENSE_TINYINT" => Column.make(col.name, CodecType.DENSE_TINYINT)
                case "DENSE_STRING" => Column.make(col.name, CodecType.DENSE_STRING, col.options)
            }
        }

        val table = Table(
            config.table,
            cols,
            DevEnv.config.blockSize)

        TableIO.store(DevEnv.config.dataDir, table)

        val segs: HashMap[String, SegmentWriter[_]] = HashMap(cols.map { col => (col.name -> new SegmentWriter(0, table.blockSize, table.name, col)(DevEnv)) }:_*)

        for (line <- lines) {
            val vals = line.split(",").map(_.trim)

            for (idx <- 0 until vals.size) {
                val segName = cols(idx).name
                val seg = segs(segName)

                if (seg.remaining > 0) {
                    seg.write(vals(idx))
                } else {
                    seg.close()
                    segs(segName) = seg.newSegment()
                    segs(segName).write(vals(idx))
                }
            }
        }

        for ((_, seg) <- segs) {
            seg.close()
        }
    }
}