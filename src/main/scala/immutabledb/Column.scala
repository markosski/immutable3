package immutabledb

import immutabledb._
import immutabledb.codec._
import util.StringUtils._

/**
  * Created by marcin on 3/8/17.
  */

case class Column(name: String, codec: CodecType.Codec, dtypeAttrs: Map[String, String] = Map()) {
    // def fqn = s"$tableName.$name"
}

object Column {
  def getCodec(column: Column): Codec[_] = column.codec match {
    case CodecType.DENSE_INT => DenseCodecInt
    case CodecType.DENSE_TINYINT => DenseCodecTinyInt
    case CodecType.DENSE_STRING => new DenseCodecString(column.dtypeAttrs("size").toInt)
    case CodecType.PFOR_INT => PFORCodecInt
    case _ => throw new Exception("")
  }
}