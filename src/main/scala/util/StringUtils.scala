package util

/**
  * Created by marcin1 on 3/20/17.
  */
object StringUtils {
    implicit def addPathSeparator(s: String) = new {
        def /(z: String) = s + java.io.File.separatorChar + z
    }
}
