

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % "2.4.5"
lazy val root = (project in file("."))
  .settings(
    name := "hello-spark",
    assembly / mainClass := Some("Main")
  )
