package com.test.common

import java.text.SimpleDateFormat
import java.util.Properties

import com.test.util.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

class BaseProgram extends App {
  lazy val paramMap:Map[String,String] = StringUtils.jsonStrToMap(args).asInstanceOf[Map[String,String]]
  var runPattern:String = _
  var topic:String = _

  val runPatternList = List("local","test","public")
  var conf:Configuration = _
  lazy val sEnv: StreamExecutionEnvironment = initFlinkStreamEnvironment()
  var kafkaProps:Properties = new Properties()
  val sdf = new SimpleDateFormat("yyyyMMddHHmm")

  def init(): Unit ={
    System.setProperty("scala.time","")
    delayedInit(sEnv.execute(this.getClass.getSimpleName))
  }

  def initParams():Unit ={
    runPattern = paramMap.getOrElse("run_pattern","")
    val zookeeperHost = paramMap.getOrElse("zookeeper_host","")
    val kafkaBroker = paramMap.getOrElse("kafka_broker","")
    val group = paramMap.getOrElse("group","")
    topic = paramMap.getOrElse("topic","")

    kafkaProps.setProperty("zookeeper.connect", zookeeperHost)
    kafkaProps.setProperty("bootstrap.servers", kafkaBroker)
    kafkaProps.setProperty("group.id", group)
  }

  def initFlinkStreamEnvironment():StreamExecutionEnvironment = {

    StreamExecutionEnvironment.getExecutionEnvironment
  }

  def getKafkaConsumer(topic:String = topic, kafkaProps:Properties = kafkaProps):FlinkKafkaConsumer08[String] ={
    new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema(), kafkaProps)
  }

  init()
  initParams()
}
