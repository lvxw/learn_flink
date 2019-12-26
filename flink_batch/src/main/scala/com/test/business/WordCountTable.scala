package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.api.scala._

/**
  main args:
      --inputDir data/input/1.txt
      --outputDir data/output
      --fs.overwrite-files true
      --fs.output.always-create-directory true
      --fs.default-scheme file://
  */
object WordCountTable extends FlinkBaseProgram{
  case class WordCount(word:String, num:Int)

  override def execute(): Unit = {
    val dataSet = env.readTextFile(conf.getString("inputDir", null))
      .flatMap ( _.split("\\s+"))
      .map (WordCount(_, 1))

    tEnv.registerDataSet("wordcount", dataSet)

    val table = tEnv.sqlQuery("select word, sum(num) from wordcount group by word")
    val result = tEnv.toDataSet[WordCount](table)

    result.print()
//    result.writeAsCsv(conf.getString("outputDir", null),"\n",",",WriteMode.OVERWRITE)
//    env.execute(jobName)
  }
}
