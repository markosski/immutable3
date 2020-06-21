package immutabledb

import immutabledb._
import immutabledb.codec._
import util.StringUtils._
import ujson.Value
import scala.collection.mutable.LinkedHashMap

/**
  * Created by marcin on 3/8/17.
  */

object ColumnType extends Enumeration {
  type ColumnType = Value
  val INT, TINYINT, STRING = Value
}

case class Column(name: String, columnType: ColumnType.ColumnType, codec: CodecType.Codec, dtypeAttrs: Map[String, String] = Map())

object Column {
  def toJsonValue(col: Column): Value = {
      val dtypeAttrs = LinkedHashMap(col.dtypeAttrs.toList.map(pair => (pair._1, ujson.Str(pair._2))):_*)
      ujson.Obj(
        "name" -> ujson.Str(col.name), 
        "columnType" -> ujson.Str(col.columnType.toString()),
        "codec" -> ujson.Str(col.codec.toString),
        "dtypeAttrs" -> (if (dtypeAttrs.isEmpty) ujson.Obj() else ujson.Obj(dtypeAttrs.head, dtypeAttrs.toList.tail:_*))
        )
  }

  def fromJsonValue(jsonValue: Value): Column = {
    Column(
      jsonValue.obj("name").str,
      ColumnType.withName(jsonValue.obj("columnType").str),
      CodecType.withName(jsonValue.obj("codec").str),
      jsonValue.obj("dtypeAttrs").obj.mapValues(v => v.str).toMap
    )
  }

  def fromJson(jsonString: String): Column = {
    val json = ujson.read(jsonString)
    fromJsonValue(json)
  }

  def make(name: String, codec: CodecType.Codec, dtypeAttrs: Map[String, String] = Map()) = {
    val columnType = codec match {
      case CodecType.DENSE_INT => ColumnType.INT
      case CodecType.PFOR_INT => ColumnType.INT
      case CodecType.DENSE_TINYINT => ColumnType.TINYINT
      case CodecType.DENSE_STRING => ColumnType.STRING
      case _ => throw new Exception("")
    }

    Column(name, columnType, codec, dtypeAttrs)
  }

  def getCodec(column: Column): Codec[_] = column.codec match {
    case CodecType.DENSE_INT => DenseCodecInt
    case CodecType.DENSE_TINYINT => DenseCodecTinyInt
    case CodecType.DENSE_STRING => new DenseCodecString(column.dtypeAttrs("size").toInt)
    case CodecType.PFOR_INT => PFORCodecInt
    case _ => throw new Exception("")
  }
}