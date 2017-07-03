package immutabledb

import column.Column

/**
  * Created by marcin1 on 3/14/17.
  */
object Table {
    /**
      * Creates directory structure for table
      * @param name
      */
    def create(name: String) = {

    }
}
class Table(name: String) {
    private var columns: Seq[Column] = List()

    def addColumn(column: Column) = {
        columns = columns :+ column
    }
}
