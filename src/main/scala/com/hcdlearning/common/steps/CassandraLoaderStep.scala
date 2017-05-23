class CassandraLoaderStep (
  val keyspaceName: String,
  val tableName: String,
  val whereCql: String
) extends BaseStep {

  import spark.implicits._

  override def execute() {
    println("Execute CassandraLoaderStep")

    var df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspaceName, "table" -> tableName, "pushdown" -> "true"))
      .load()

      if (!whereCql.isEmpty) {
        df = df.filter(whereCql)
      }

  }

}

object CassandraLoaderStep {
  def apply(keyspaceName: String, tableName: String) = {
    new CassandraLoaderStep(
      keyspaceName,
      tableName,
      ""
    )
  }
}