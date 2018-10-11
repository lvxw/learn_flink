package com.test.common

import com.test.util.StringUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

class BaseProgram extends App {
  lazy val paramMap:Map[String,String] = StringUtils.jsonStrToMap(args).asInstanceOf[Map[String,String]]
  var runPattern:String = _
  var inputDir:String = _
  var outputDir:String = _
  var dataDate:String = _
  val appName = this.getClass.getSimpleName.replace("$", "")

  val runPatternList = List("local","test","public")
  var flinkConf:Configuration = _
  var executionEnvironment: ExecutionEnvironment = _

  def init(): Unit ={
    System.setProperty("scala.time","")
    delayedInit(executionEnvironment.execute(this.getClass.getSimpleName))
  }

  def initParams():Unit ={
    runPattern = paramMap.getOrElse("run_pattern","")
    inputDir = paramMap.getOrElse("input_dir","")
    outputDir = paramMap.getOrElse("output_dir","")
    dataDate = paramMap.getOrElse("data_date","")
  }

  def initFlinkConf():Unit = {
    flinkConf = new Configuration()
    flinkConf.setBoolean("fs.overwrite-file", true)
    flinkConf.setBoolean("fs.output.always-create-directory", true)
    flinkConf.setString("fs.default-scheme", "hdfs://artemis-02:9000/")

    executionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    if(runPattern == runPatternList(0)){
      flinkConf.setString("fs.default-scheme","file:///")
      executionEnvironment = ExecutionEnvironment.createLocalEnvironment(flinkConf)
    }
  }

  init()
  initParams()
  initFlinkConf()

}
