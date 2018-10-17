package com.test.common

import com.test.util.ParamUtils
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api._

class BaseProgram extends App {
  lazy val paramMap:Map[String,String] = ParamUtils.jsonStrToMap(args.mkString("")).asInstanceOf[Map[String,String]]
  var runPattern:String = _
  var inputDir:String = _
  var outputDir:String = _
  var dataDate:String = _

  val runPatternList = List("local","test","public")
  val env: ExecutionEnvironment = getLinkEnvironment()
  lazy val tEnv = TableEnvironment.getTableEnvironment(env)

  def init(): Unit ={
    System.setProperty("scala.time","")
    delayedInit(env.execute(this.getClass.getSimpleName))
  }

  def initParams():Unit ={
    runPattern = paramMap.getOrElse("run_pattern","")
    inputDir = paramMap.getOrElse("input_dir","")
    outputDir = paramMap.getOrElse("output_dir","")
    dataDate = paramMap.getOrElse("data_date","")
  }

  def getLinkEnvironment():ExecutionEnvironment = {
    val conf = new Configuration()
    conf.setBoolean("fs.overwrite-file", true)
    conf.setBoolean("fs.output.always-create-directory", true)
    conf.setString("fs.default-scheme", "hdfs://artemis-02:9000/")

    if(runPattern == runPatternList(0)){
      conf.setString("fs.default-scheme","file:///")
      return ExecutionEnvironment.createLocalEnvironment(conf)
    }
    ExecutionEnvironment.getExecutionEnvironment
  }

  init()
  initParams()
}
