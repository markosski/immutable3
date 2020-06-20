package immutabledb

import java.io._
import util.StringUtils._
import java.nio.file.Files
import java.nio.file.Path
import ujson.Value

case class Table(name: String, columns: List[Column], blockSize: Int) {
  def getColumn(colName: String): Column = columns
    .filter(c => c.name == colName)
    .headOption
    .fold(throw new Exception(s"Column $colName does not exist in table ${name}"))(x => x)
}

// https://gist.github.com/ramn/5566596
class ObjectInputStreamWithCustomClassLoader(
  fileInputStream: FileInputStream
) extends ObjectInputStream(fileInputStream) {
  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
    try { Class.forName(desc.getName, false, getClass.getClassLoader) }
    catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
  }
}

object TableIO {
    val fileName = "_table.meta"

    def toJsonValue(table: Table): Value = {
      ujson.Obj(
        "name" -> ujson.Str(table.name), 
        "columns" -> ujson.Arr(table.columns.map(x => Column.toJsonValue(x)):_*),
        "blockSize" -> ujson.Num(table.blockSize)
        )
    }

    def fromJsonValue(jsonValue: Value): Table = {
      Table(
        jsonValue.obj("name").str,
        jsonValue.obj("columns").arr.map(x => Column.fromJsonValue(x)).toList,
        jsonValue.obj("blockSize").num.toInt
      )
    }

    def load(dataDir: String, tableName: String): Table = {
        val jsonValue = ujson.read(new File(List(dataDir, tableName, fileName).mkString("/")))
        fromJsonValue(jsonValue)
    }

    def store(dataDir: String, table: Table) = {
        val path = List(dataDir, table.name).mkString("/")
        Files.createDirectories(new File(path).toPath)

        val fileWriter = new FileWriter(List(path, fileName).mkString("/"))

        val json = toJsonValue(table)
        ujson.writeTo(json, fileWriter)
        fileWriter.close()
    }

    def clear(dataDir: String, table: Table): Unit = {
        val path: File = new File(List(dataDir, table.name).mkString("/"))
        for (f <- Option(path.listFiles()).fold(Array[File]())(identity)) {
          if (f.isFile()) f.delete()
        }
    }
}