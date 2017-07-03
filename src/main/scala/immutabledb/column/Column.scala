package immutabledb.column

import immutabledb.codec._
import immutabledb.Config
import util.StringUtils._

/**
  * Created by marcin on 3/8/17.
  */

case class Column(name: String, tableName: String) {
    def fqn = s"$tableName.$name"
    def path = Config.dataDir / tableName / name
}
