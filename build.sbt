name := "spark-etl"

version := "1.5.1"

scalaVersion := "2.11.8"

scapegoatVersion := "1.1.0"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.4",
  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)


val listSuites = taskKey[Unit]("list all test suites")
listSuites := {
  val tests = (definedTests in Test).value
  tests map { t =>
    println(t.name)
  }
}