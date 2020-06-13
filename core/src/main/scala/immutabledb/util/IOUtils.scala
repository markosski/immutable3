package immutabledb.util

/**
  * Created by marcin1 on 3/9/17.
  */

import java.io.Closeable
import java.io.File

object IOUtils {
    def dowith[A <: Closeable](resource: A, expr: => Unit)  = {
        expr
        resource.close
    }

    def createDir(path: String): Unit = {
        val dir = new File(path)
        dir.mkdirs
    }
}
