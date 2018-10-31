import sbt.Keys.libraryDependencies

name := "TestSBT"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.2" % "provided",

  "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided",

  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.2",

  "org.apache.spark" % "spark-mllib_2.11" % "2.3.2" % "provided",

  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"

)


libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.5" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.5" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.5" % "provided"


assemblyJarName in assembly := "kafka.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
