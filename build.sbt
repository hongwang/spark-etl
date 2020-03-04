name := "spark-etl"

version := "1.7.0"

scalaVersion := "2.11.12"

scapegoatVersion := "1.1.0"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  // "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  // "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.4",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "7.2.0",
  "com.typesafe" % "config" % "1.3.4",
  "org.scalactic" %% "scalactic" % "3.0.5" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)


val listSuites = taskKey[Unit]("list all test suites")
listSuites := {
  val tests = (definedTests in Test).value
  tests map { t =>
    println(t.name)
  }
}