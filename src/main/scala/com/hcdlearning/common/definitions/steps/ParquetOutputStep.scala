package com.hcdlearning.common.definitions.steps

import com.hcdlearning.common.ExecuteContext
import org.apache.spark.sql.SparkSession

class ParquetOutputStep(
  name: String,
  mode: String,
  path: String
) extends BaseStep(name) {

  templateFields += ("path" -> path)
  //val template_fields = Seq("path")

  // override def renderTemplates(engine: BaseTemplateEngine ,templateContext: Map[String, String]) = {
  //   templateContext + ("name" -> name)
  //   _path = engine.render(_path, templateContext)
  //   println(path)
  // }

  override def execute(ctx: ExecuteContext) {
    val saveTo = templateFields.getOrElse("path", path)
    logInfo(s"save parquet to $saveTo")

    ctx.df.write.mode(mode).parquet(saveTo)
  }

}