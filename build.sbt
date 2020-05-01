name := "immuTable3"

version := "1.0"

scalaVersion := "2.12.11"

resolvers += "Sonatype OSS Snapshots" at
        "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "me.lemire.integercompression" % "JavaFastPFOR" % "0.1.10"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.5.0"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.22"
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.6.29"
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2"
libraryDependencies += "org.iq80.snappy" % "snappy" % "0.4"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"