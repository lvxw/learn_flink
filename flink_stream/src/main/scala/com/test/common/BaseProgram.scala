package com.test.common

import java.util.Properties

import com.test.util.ParamUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class BaseProgram extends App {
  val sEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val kafkaProps:Properties = new Properties()
  val className = this.getClass.getSimpleName.replace("$", "")

  val runPatternList = List("local","test","public")
  var mainArgsMap:Map[String,String] = _
  var fixedParamMap:Map[String,Any] = _
  var runPattern:String = _
  var topic:String = _
  var graphiteMap:Map[String,String]= _
  var redisMap:Map[String,String] = _

  lazy val kafkaConsumer:FlinkKafkaConsumer08[String] = getKafkaConsumer()
  lazy val graphiteSink = new GraphiteSink[(String, String)](graphiteMap.getOrElse("graphite_host",""),graphiteMap.getOrElse("graphite_port","0").toInt,graphiteMap.getOrElse("graphite_batchSize","0").toInt)
  lazy val redisSink:RedisSink[(String, String)] = new RedisSink[(String, String)](
    new FlinkJedisPoolConfig.Builder().setHost(redisMap.getOrElse("redis_host","localhost")).setPort(redisMap.getOrElse("redis_port","6379").toInt).setDatabase(redisMap.getOrElse("redis_db","0").toInt).build(),
    new RedisMapperImpl()
  )

  def init(): Unit ={
    mainArgsMap = ParamUtils.jsonStrToMap(args.mkString("")).asInstanceOf[Map[String,String]]
    fixedParamMap = ParamUtils.getClassPathFileContent("fixed-params.conf")
    delayedInit(sEnv.execute(className))
  }

  def initParams():Unit ={
    runPattern = mainArgsMap.getOrElse("run_pattern","")
    topic = mainArgsMap.getOrElse("topic","")
    val group = mainArgsMap.getOrElse("group",className)

    val testOrProductParamMap = if(runPattern==runPatternList(0) || runPattern==runPatternList(1)){
      fixedParamMap.get(runPatternList(1)).get.asInstanceOf[Map[String,Any]]
    }else{
      fixedParamMap.get(runPatternList(2)).get.asInstanceOf[Map[String,Any]]
    }
    val zookeeperKafkaMap = testOrProductParamMap.get("zk_kafka").get.asInstanceOf[Map[String,String]]
    graphiteMap = testOrProductParamMap.get("graphite").get.asInstanceOf[Map[String,String]]
    redisMap = testOrProductParamMap.get("redis").get.asInstanceOf[Map[String,String]]

    kafkaProps.setProperty("zookeeper.connect", zookeeperKafkaMap.getOrElse("zk_connect",""))
    kafkaProps.setProperty("bootstrap.servers", zookeeperKafkaMap.getOrElse("kafka_broker",""))
    kafkaProps.setProperty("group.id", group)
  }

  def getKafkaConsumer(topic:String = topic, kafkaProps:Properties = kafkaProps):FlinkKafkaConsumer08[String] ={
    new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema(), kafkaProps)
  }

  init()
  initParams()
}

class RedisMapperImpl extends RedisMapper[(String, String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.RPUSH)
  }

  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2
}

