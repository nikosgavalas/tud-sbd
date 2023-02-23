name := "Lab 1"
version := "1.0"
scalaVersion := "2.12.14"

scalastyleFailOnWarning := true

fork in run := true

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.uber" % "h3" % "4.0.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyCacheOutput in assembly := false
assemblyJarName in assembly := "lab1.jar"
