name := "events-pipeline"

version := "0.1"

scalaVersion := "2.11.7"
val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.1.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1"

)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)