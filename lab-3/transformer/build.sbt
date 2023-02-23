name := "Transformer"
version := "1.0"
scalaVersion := "2.13.6"

scalastyleFailOnWarning := true

run / fork := true
run / connectInput := true
outputStrategy := Some(StdoutOutput)

libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-simple" % "1.8.0-beta4",
    "org.apache.kafka" %% "kafka-streams-scala" % "3.0.0",
    "io.github.azhur" %% "kafka-serde-circe" % "0.6.5",
    "io.circe" %% "circe-generic" % "0.14.1"
)
