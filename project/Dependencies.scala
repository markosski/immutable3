import sbt._

object Dependencies {
    lazy val pfor = "me.lemire.integercompression" % "JavaFastPFOR" % "0.1.10"
    lazy val roaring = "org.roaringbitmap" % "RoaringBitmap" % "0.6.29"
    lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.8.2"
    lazy val snappy = "org.iq80.snappy" % "snappy" % "0.4"
    lazy val scopt = "com.github.scopt" %% "scopt" % "4.0.0-RC2"
    lazy val parsers = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
    lazy val logging = Seq(
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "org.slf4j" % "slf4j-log4j12" % "1.7.22")
}
