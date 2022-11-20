name := "pay-baymx-challenge"

version := "0.1"

scalaVersion := "2.12.7"
val flinkVersion = "1.12.3"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion,

  "org.json4s" %% "json4s-native" % "3.6.1",
  "joda-time" % "joda-time" % "2.10.10",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.scalatest" %% s"scalatest" % "3.0.3" % "test")


lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )
