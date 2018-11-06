package com.test.common

import java.util.Properties

import com.test.util.ParamUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class BaseProgram extends App {
  val kafkaProps:Properties = new Properties()
  val className = this.getClass.getSimpleName.replace("$", "")

  val runPatternMap = Map(1->"local",2->"test",3->"public")
  val consumeTypeMap = Map(1 -> "earliest",2->"latest",3->"croupOffsets",4->"specificOffsets")
  val checkpointModeMap = Map(1 -> "EXACTLY_ONCE",2->"AT_LEAST_ONCE")
  var mainArgsMap:Map[String,String] = _
  var fixedParamMap:Map[String,Any] = _
  var runPattern:String = _
  var topic:String = _
  var checkpointInterval:Long =_
  var checkpointMode:CheckpointingMode =_
  var graphiteMap:Map[String,String]= _
  var redisMap:Map[String,String] = _

  lazy val sEnv = getStreamEnvironment()
  lazy val graphiteSink = new GraphiteSink[(String, String,Long)](graphiteMap.getOrElse("graphite_host",""),graphiteMap.getOrElse("graphite_port","0").toInt,graphiteMap.getOrElse("graphite_batchSize","0").toInt)
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
    checkpointInterval = mainArgsMap.getOrElse("checkpoint_interval","-1").toLong
    checkpointMode = if(mainArgsMap.getOrElse("checkpoint_mode","") == checkpointModeMap.get(1).get) CheckpointingMode.EXACTLY_ONCE else CheckpointingMode.AT_LEAST_ONCE
    val group = mainArgsMap.getOrElse("group",className)

    val testOrProductParamMap = if(runPattern==runPatternMap.get(1).get || runPattern==runPatternMap.get(2).get){
      fixedParamMap.get(runPatternMap.get(2).get).get.asInstanceOf[Map[String,Any]]
    }else{
      fixedParamMap.get(runPatternMap.get(3).get).get.asInstanceOf[Map[String,Any]]
    }
    val zookeeperKafkaMap = testOrProductParamMap.get("zk_kafka").get.asInstanceOf[Map[String,String]]
    graphiteMap = testOrProductParamMap.get("graphite").get.asInstanceOf[Map[String,String]]
    redisMap = testOrProductParamMap.get("redis").get.asInstanceOf[Map[String,String]]

    kafkaProps.setProperty("zookeeper.connect", zookeeperKafkaMap.getOrElse("zk_connect",""))
    kafkaProps.setProperty("bootstrap.servers", zookeeperKafkaMap.getOrElse("kafka_broker",""))
    kafkaProps.setProperty("group.id", group)
  }

  def getStreamEnvironment(): StreamExecutionEnvironment ={
    val sEnv =  StreamExecutionEnvironment.getExecutionEnvironment
    if(checkpointInterval > 0){
      sEnv.enableCheckpointing(checkpointInterval)
      sEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    }
    sEnv
  }

  def getKafkaConsumer(
        topic:String = topic,
        kafkaProps:Properties = kafkaProps,
        consumeType:String = consumeTypeMap.getOrElse(3,"latest"),
        specificStartupOffsets:java.util.Map[KafkaTopicPartition,java.lang.Long] = null
      ):FlinkKafkaConsumer08[String] ={
    val kafkaConsumer = new FlinkKafkaConsumer08[String](topic, new SimpleStringSchema(), kafkaProps)
    if(consumeType == consumeTypeMap.get(1).get){
      kafkaConsumer.setStartFromEarliest()
    }else if(consumeType == consumeTypeMap.get(2).get){
      kafkaConsumer.setStartFromLatest()
    }else if(consumeType == consumeTypeMap.get(3).get){
      kafkaConsumer.setStartFromGroupOffsets()
    }else{
      kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets)
    }
    kafkaConsumer
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

