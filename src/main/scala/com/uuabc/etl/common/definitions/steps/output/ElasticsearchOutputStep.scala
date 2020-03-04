package com.uuabc.etl.common.definitions.steps.output


import com.typesafe.config.Config
import com.uuabc.etl.common.definitions.steps.{BaseOutputStep, BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.definitions.steps.ConfigImplicits._
import com.uuabc.etl.common.execution.ExecuteContext
import org.apache.spark.sql.{DataFrame, SaveMode}

class ElasticsearchOutputStep(
  name: String,
  index: String,
  docId: String,
  upstream: Option[String] = None
) extends BaseOutputStep(name, upstream) {

  protected def this() = this(null, null, null)  // For reflect only

  override def execute(ctx: ExecuteContext, df: DataFrame): Unit = {
    df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.mapping.id", docId)
      .option("es.index.auto.create", "no")
      .option("es.spark.dataframe.write.null", "true")
      .mode(SaveMode.Overwrite)
      .save(index)
  }
}



object ElasticsearchOutputStep extends IStepConfigSupported {

  override def use(config: Config): BaseStep = {

    val name = config.getString("name")
    val index = config.getString("index")
    val docId = config.getString("docId")
    val upstream = config.getStringOpt("upstream")

    new ElasticsearchOutputStep(name, index, docId, upstream)
  }
}