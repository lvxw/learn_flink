package com.test.common

import com.test.business.WordCount2.conf
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.TableEnvironment
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.flink.api.scala._

object FlinkBaseProgram{
  scala.util.Properties.setProp("scala.time", "true")
  implicit def hadoopUser:String = "root"
  implicit class implicitHdfsOutput[K, V](dataSet :DataSet[(K,V)]){
    def saveToHdfs(outputDir:String = conf.getString("outputDir", null))(implicit hadoopUser:String) :Unit = {
      System.setProperty("HADOOP_USER_NAME", hadoopUser)
      val job = Job.getInstance()
      val hadoopOutputFormat = new HadoopOutputFormat[K, V](new TextOutputFormat[K,V], job)

      FileOutputFormat.setOutputPath(job, new Path(outputDir))
      dataSet.output(hadoopOutputFormat)
    }
  }
  implicit class implicitHdfsInput(env: ExecutionEnvironment){
    def readFromHdfs(inputDir:String = conf.getString("inputDir", null))(implicit hadoopUser:String): DataSet[(LongWritable, Text)] = {
      System.setProperty("HADOOP_USER_NAME", hadoopUser)
      env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(), key=classOf[LongWritable],classOf[Text], inputDir, Job.getInstance()))
    }
  }
}

abstract class FlinkBaseProgram extends App {
  protected val conf : Configuration = ParameterTool.fromArgs(args).getConfiguration
  lazy protected val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  lazy protected val tEnv = TableEnvironment.getTableEnvironment(env)
  lazy protected val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
  lazy protected  val jobName = this.getClass.getSimpleName.replaceFirst("\\$$","")

  protected def getFlinkKafkaConsumer(consumerType:String="earliest", zkConnect:String=null, kafkaServers:String=null, groupId:String=null, topic:String=null): FlinkKafkaConsumer[String] = {
    val props = new java.util.Properties()
    props.put("zookeeper.connect", conf.getString("zookeeper.connect", zkConnect))
    props.put("bootstrap.servers", conf.getString("bootstrap.servers", kafkaServers))
    props.put("group.id", conf.getString("group.id", groupId))

    val methodName = s"setStartFrom${consumerType.substring(0,1).toUpperCase()}${consumerType.substring(1).toLowerCase()}"
    if(List("setStartFromEarliest", "setStartFromLatest", "setStartFromCroupOffsets", "setStartFromSpecificOffsets").contains(methodName)){
      val flinkKafkaConsumer = new FlinkKafkaConsumer[String](conf.getString("topic", topic),new SimpleStringSchema(), props)
      flinkKafkaConsumer.getClass.getMethod(methodName).invoke(flinkKafkaConsumer)
      flinkKafkaConsumer
    }else{
      throw new RuntimeException(s"Flink does not support this type of consumption: ${consumerType}, you can select one from [earliest, latest, croupOffsets, specificOffsets]")
    }
  }

  def execute(): Unit

  execute()
}
