package com.uuabc.etl.common.builder

import java.nio.file.{Files, Paths}
import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigResolveOptions}
import com.uuabc.etl.common.{ETLException, Logging}
import com.uuabc.etl.common.definitions.Recipe
import com.uuabc.etl.common.definitions.steps.{BaseStep, IStepConfigSupported}
import com.uuabc.etl.common.util.{HDFSClient, ReflectUtils}


object RecipeBuilder {
  private val STEP_PACKAGE_PREFIX = "com.uuabc.etl.common.definitions.steps"
  private val STEP_PACKAGES = Map(
    "input" -> (STEP_PACKAGE_PREFIX + ".input"),
    "trans" -> (STEP_PACKAGE_PREFIX + ".trans"),
    "output" -> (STEP_PACKAGE_PREFIX + ".output")
  )

  def from(
    recipeFile: String,
    deployMode: String = "cluster"): Recipe = {
    new RecipeBuilder(recipeFile, deployMode).build
  }
}

class RecipeBuilder(
  recipeFile: String,
  deployMode: String = "cluster"
) extends Logging {

  private val recipeConf = {

    if (recipeFile == null || recipeFile.trim.isEmpty) {
      throw new ETLException("please specify recipe file")
    }

    logInfo(s"loading recipe: $recipeFile")

//    var config = deployMode match {
//      case "cluster" =>
//        ConfigFactory.parseString(HDFSClient.readFile(recipeFile))
//      case "client" =>
//        ConfigFactory.parseFile(new File(recipeFile))
//    }

    val content = deployMode match {
      case "cluster" =>
        HDFSClient.readFile(recipeFile)
      case "client" =>
        new String(Files.readAllBytes(Paths.get(recipeFile)))
    }

    // variables substitution / variables resolution order:
    // config file --> system environment --> java properties
    val config = ConfigFactory.parseString(content)
      .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      //.resolveWith(ConfigFactory.systemProperties, ConfigResolveOptions.defaults.setAllowUnresolved(true))

    val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
    logInfo("[INFO] parsed config file: " + config.root().render(options))

    config
  }

  def build: Recipe = {
    val app = getAppConfig
    val inputs = getInputs
    val trans = getTrans
    val outputs = getOutput

    val steps = inputs ++ trans ++ outputs
    new Recipe(app.getString("name"), steps)
  }

  private def getAppConfig: Config = {
    recipeConf.getConfig("app")
  }

  private def getInputs: List[BaseStep] = {
    getSteps("input")
  }

  private def getTrans: List[BaseStep] = {
    getSteps("trans")
  }

  private def getOutput: List[BaseStep] = {
    getSteps("output")
  }

  private def getSteps(stepType: String): List[BaseStep] = {
    var steps = List[BaseStep]()

    recipeConf
      .getConfigList(stepType)
      .foreach(stepConf => {
        val className = buildClassFullQualifier(stepConf.getString("type"), stepType)

        //        way 1:
        //        val step = Class
        //          .forName(className)
        //          .newInstance()
        //          .asInstanceOf[BaseStep]
        //        step.setConf(stepConf)

        //        way 2:
        //        val constructor = Class.forName(className).getDeclaredConstructor(classOf[Config])
        //        val step = constructor.newInstance(stepConf).asInstanceOf[BaseStep]

        val configurator = ReflectUtils.getCompanion[IStepConfigSupported](className)
        val step = configurator.use(stepConf)

        steps = steps :+ step
      })

    steps
  }

  private def buildClassFullQualifier(name: String, stepType: String): String = {

    val qualifier = name
    if (qualifier.indexOf(".") > 1) {
      return qualifier
    }

    val packageName = RecipeBuilder.STEP_PACKAGES.get(stepType).orNull
    if (packageName == null) {
      throw new ETLException(s"cannot find step's namespace: $stepType")
    }

    val fullQualifier = packageName + "." + qualifier.toLowerCase + stepType + "step"
    val services: Iterable[BaseStep] = ServiceLoader.load(classOf[BaseStep]).asScala
    for (service <- services) {
      val clz = service.getClass
      if (clz.getName.toLowerCase == fullQualifier) {
        return clz.getName
      }
    }

    throw new ETLException(s"cannot find step's qualifier: $fullQualifier")
  }
}
