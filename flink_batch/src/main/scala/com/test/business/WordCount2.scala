package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

/**
  main args:
      --inputDir hdfs://master:9000/lvxuewen/1
      --outputDir hdfs://master:9000/lvxuewen/output/
      --fs.overwrite-files true
      --fs.output.always-create-directory true
      --fs.default-scheme hdfs://master:9000/
      --fs.defaultFS hdfs://master:9000/
  */
object WordCount2 extends FlinkBaseProgram{

  override def execute(): Unit = {

    import FlinkBaseProgram._
    env.setParallelism(1)

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", conf.getString("fs.defaultFS", "hdfs://master:9000/"))

    System.setProperty("HADOOP_USER_NAME", hadoopUser)
    val dataStream = env.readFromHdfs()
      .flatMap(el => el._2.toString.split("\\s+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .map(el =>(new Text(el._1), new LongWritable(el._2)))

//    dataStream.writeAsCsv(conf.getString("outputDir", null),"\n",",",  WriteMode.OVERWRITE)

    dataStream.saveToHdfs()
    env.execute(jobName)
  }
}
