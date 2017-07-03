package immutabledb

import util.StringUtils._

/**
  * Created by marcin1 on 3/14/17.
  */
object Config {
    def dataDir: String = "" / "Users" / "marcin" / "immutable3"
    def blockSize: Int = 1024
    def vectorSize: Int = 1024
    def segmentSize: Int = 512
    def readBufferSize: Int = 1024
}
