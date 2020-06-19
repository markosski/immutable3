package immutabledb

object Row {
    def fromSeq(xs: Seq[Any]): Row = Row(xs:_*)
}

case class Row(xs: Any*) {
    def getByte(idx: Int) = xs(idx).asInstanceOf[Byte]
    def getInt(idx: Int) = xs(idx).asInstanceOf[Int]
    def getFloat(idx: Int) = xs(idx).asInstanceOf[Float]
    def getDouble(idx: Int) = xs(idx).asInstanceOf[Double]
    def getString(idx: Int) = xs(idx).asInstanceOf[String]
    override def toString: String = xs.mkString("Row(", ",", ")")
}