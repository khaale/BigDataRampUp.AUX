name := "BigDataRampUp.Hdfs2Kafka"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.9.0.1",
  "org.apache.spark" %% "spark-core" % "1.6.0"
)

