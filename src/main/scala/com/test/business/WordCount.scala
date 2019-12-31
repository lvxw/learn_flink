package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.hadoop.io.{LongWritable, Text}
/**
  main args:
      --inputDir data/input/1.txt
      --outputDir data/output/
      --fs.overwrite-files true
      --fs.output.always-create-directory true
      --fs.default-scheme file://
  */

/**
  main args:
      --inputDir hdfs://master:9000/lvxuewen/1
      --outputDir hdfs://master:9000/lvxuewen/output/
      --fs.overwrite-files true
      --fs.output.always-create-directory true
  */
object WordCount extends FlinkBaseProgram{

  override def execute(): Unit = {
    import FlinkBaseProgram._
    env.setParallelism(1)
    val text = env.readTextFile(conf.getString("inputDir", null))
    val result = text.flatMap( _.split("\\s+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .map(el =>(new Text(el._1), new LongWritable(el._2)))

//    result.writeAsCsv(conf.getString("outputDir", null),"\n",",",  WriteMode.OVERWRITE)
    result.saveToHdfs(conf.getString("outputDir", "hdfs://master:9000/lvxuewen/output/"))
    env.execute(jobName)
  }
}
