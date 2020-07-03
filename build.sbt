import sbt.util

name := "CNA"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

logLevel := util.Level.Debug

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "net.liftweb" %% "lift-json" % "3.4.1"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
