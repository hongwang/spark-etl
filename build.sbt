name := "spark-etl"

version := "1.0"

scalaVersion := "2.11.8"

scapegoatVersion := "1.1.0"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)
