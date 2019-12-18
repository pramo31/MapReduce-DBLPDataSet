ThisBuild / organization := "com.cloud.mapreduce"
ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version      := "0.1.0"
ThisBuild / name      := "MapReduce"

lazy val root = (project in file("."))
  .settings(
	libraryDependencies ++= Seq("ch.qos.logback" % "logback-examples" % "1.3.0-alpha4","com.typesafe" % "config" % "1.2.1", "org.apache.hadoop" % "hadoop-common" % "2.8.5", "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.5", "org.apache.hadoop" % "hadoop-client" % "2.8.5", "org.apache.hadoop" % "hadoop-hdfs" % "2.8.5", "javax.xml.stream" % "stax-api" % "1.0-2", "commons-lang" % "commons-lang" % "2.6", "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0","com.ctc.wstx" % "woodstox-osgi" % "3.2.1.1", "org.scalatest" %% "scalatest" % "3.0.8" % "test")
  )
  
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}