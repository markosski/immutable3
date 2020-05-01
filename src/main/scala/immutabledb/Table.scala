package immutabledb

import java.io._
import util.StringUtils._
import java.nio.file.Files
import java.nio.file.Path

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
    def load(dataDir: String, tableName: String): Table = {
        val fileIn = new FileInputStream(new File(dataDir / tableName / fileName))
        val objectIn = new ObjectInputStreamWithCustomClassLoader(fileIn)
        val s = objectIn.readObject().asInstanceOf[Table]
        objectIn.close
        fileIn.close
        s
    }

    def store(dataDir: String, table: Table) = {
        val path = new File(dataDir / table.name).toPath()
        Files.createDirectories(path)

        val fileOut = new FileOutputStream(new File(dataDir / table.name / fileName))
        val objectOut = new ObjectOutputStream(fileOut)
        objectOut.writeObject(table)
        objectOut.close
        fileOut.close
    }
}