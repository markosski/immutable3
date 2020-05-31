package immutabledb

import util.StringUtils._
import immutabledb.storage.SegmentManager

case class Config(
    dataDir: String, 
    blockSize: Int, 
    segmentSize: Int, 
    readBufferSize: Int,
    resultQueueSize: Int
    )

trait ConfigEnv {
    val config: Config
}

trait SegmentManagerEnv {
    val sm: SegmentManager
}

trait Env extends ConfigEnv

object DevEnv extends Env {
    val config = Config(
        dataDir =  "" / "Users" / "marcin" / "immutable3",
        blockSize = 1024,
        segmentSize = 100,
        readBufferSize = 1024,
        resultQueueSize = 100
    )
}