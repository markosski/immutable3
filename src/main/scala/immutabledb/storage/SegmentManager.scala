package immutabledb.storage

import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.io.RandomAccessFile
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import immutabledb._
import immutabledb.codec._
import immutabledb.{ Table, DataType, ConfigEnv }
import java.io.File
import util.StringUtils._
import com.typesafe.scalalogging.LazyLogging

case class SegmentInfo(segment: ByteBuffer, segmentMeta: SegmentMeta)

// - Need something that will have information about database strucute, e.g. find all tables in the system
// - Retrieve list of segments for the table
// todo: When loading segments validate there is the same segment cound for each column in the table
class SegmentManager(dataDir: String) extends LazyLogging {
    val tables: List[Table] = getTables()
    val segments: mutable.Map[String, List[ByteBuffer]] = getSegmentMap(tables)
    val segmentsMeta: mutable.Map[String, List[SegmentMeta]] = getSegmentMetaMap(tables)

    logger.info(s"Discovered tables: $tables")
    logger.info(s"Loaded segments: $segments")

    private[this] def getTables(): List[Table] = {
        // scan directories that contains table.meta
        val dataDirFile = new File(dataDir)
        val dirs = dataDirFile.listFiles().filter(_.isDirectory()).toList
        dirs.map { parent =>
            TableIO.load(dataDir, parent.getName)
        }
    }

    // for given table and column name return list of segments
    private[this] def getSegmentFiles(tableName: String, colName: String): List[File] = {
        new File(dataDir / tableName).listFiles().toList
            .filter(x => x.getName.startsWith(s"${colName}_") && x.getName.endsWith(".dat"))
            .sortBy(f => f.getName)
    }

    // TODO: make sure segment are sorted in the list
    private[this] def getSegmentMap(tables: List[Table]): mutable.Map[String, List[ByteBuffer]] = {
        val segments: mutable.Map[String, List[ByteBuffer]] = mutable.Map()

        tables.foreach { table =>
            table.columns.foreach { col =>
                {
                    val segmentFiles = getSegmentFiles(table.name, col.name)
                    val buffers = segmentFiles.map( segFile => getByteBuffer(segFile.getAbsolutePath()) )
                    segments += (s"${table.name}.${col.name}" -> buffers)                
                }
            }
        }
        segments
    }

    // make sure metas are sorted in the list
    private[this] def getSegmentMetaFiles(tableName: String, colName: String): List[File] = {
        new File(dataDir / tableName).listFiles().toList
            .filter(x => x.getName.startsWith(s"${colName}_") && x.getName.endsWith(".meta"))
            .sortBy(f => f.getName)
    }

    private[this] def getSegmentMetaMap(tables: List[Table]): mutable.Map[String, List[SegmentMeta]] = {
        val segmentMetas: mutable.Map[String, List[SegmentMeta]] = mutable.Map()

        tables.foreach { table =>
            table.columns.foreach { col => 
                {
                    val metas = getSegmentMetaFiles(table.name, col.name).map(f => SegmentMeta.load(f))
                    segmentMetas += (s"${table.name}.${col.name}" -> metas)
                }
            }
        }
        segmentMetas
    }

    private[this] def getByteBuffer(filePath: String): ByteBuffer = {
        val file = new RandomAccessFile(filePath, "r")
        val size: Long = file.length
        val mbuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size)
        file.close()
        mbuffer
    }

    def getTable(tableName: String): Table = tables
        .filter(t => t.name == tableName)
        .headOption
        .fold(throw new Exception(s"Table $tableName does not exist in SegmentManager"))(x => x)

    def getTableSegmentCount(tableName: String): Int = {
        val table = getTable(tableName)
        val fstCol = table.columns.head

        getSegments(tableName, fstCol.name).size
    }

    def getSegment(id: Int, tableName: String, columnName: String): Segment = {
        val tableColumn = s"$tableName.$columnName"
        val segmentMeta = segmentsMeta(tableColumn)(id)
        val segment = segments(tableColumn)(id)
        new Segment(id, segment, segmentMeta)
    }

    def getSegments(tableName: String, columnName: String): List[Segment] = {
        val tableColumn = s"$tableName.$columnName"
        (0 until segments(tableColumn).size).toList.map(idx => getSegment(idx, tableName, columnName))
    }
}
