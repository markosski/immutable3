ThisBuild / organization := "immutableDB"
ThisBuild / scalaVersion := "2.12.11"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-feature",
  "-Ypartial-unification"
  // "-Xfatal-warnings",
)
ThisBuild / version := "0.0.1-SNAPSHOT"

ThisBuild / resolvers += "Sonatype OSS Snapshots" at
        "https://oss.sonatype.org/content/repositories/snapshots"

val fatJarSettings = Seq(
  test in assembly := {},
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("assembly"))
  },
  assemblyMergeStrategy in assembly := {
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   },
  addArtifact(artifact in (Compile, assembly), assembly)
)

lazy val core = (project in file("./core"))
  .settings(
    name := "core",
    libraryDependencies ++= Seq(
      Dependencies.pfor,
      Dependencies.roaring,
      Dependencies.snappy,
      Dependencies.scopt,
      Dependencies.parsers,
      Dependencies.ujson
    )
    ++ Dependencies.logging
  )

lazy val loader = (project in file("./loader"))
  .dependsOn(core)
  .settings(
    name := "loader",
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.parsers
    )
    ++ Dependencies.logging
  )
  .settings(fatJarSettings: _*)
  .settings(mainClass in assembly := Some("immutabledb.loader.LoaderCli"))

lazy val engine = (project in file("./engine"))
  .dependsOn(core)
  .settings(
    name := "immutabledb",
    libraryDependencies ++= Seq(
      Dependencies.pfor,
      Dependencies.roaring,
      Dependencies.snappy,
      Dependencies.scopt,
      Dependencies.parsers,
      Dependencies.janino
    )
    ++ Dependencies.logging
  )
  .settings(fatJarSettings: _*)
  .settings(mainClass in assembly := Some("immutabledb.ConsoleCli"))

lazy val root = (project in file("."))
   .aggregate(loader, engine)
