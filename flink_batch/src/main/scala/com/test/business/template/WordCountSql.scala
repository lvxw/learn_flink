package com.test.business.template

import com.test.common.BaseProgram
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"input_dir\":\"flink_batch/tmp/logs/test/1\",
          \"output_dir\":\"flink_batch/tmp/out/test\",
          \"run_pattern\":\"local\"
        }
  */
object WordCountSql extends BaseProgram{
  case class WordCount(word:String, num:Long)

  val text = env.readTextFile(inputDir)
  val dataSet = text.flatMap ( _.split(","))
    .map (WordCount(_, 1))

  tEnv.registerDataSet("wordcount", dataSet)

  private val table = tEnv.sqlQuery("select word, sum(num) from wordcount group by word")
  private val result: DataSet[WordCount] = tEnv.toDataSet[WordCount](table)
  result.writeAsCsv(outputDir,"\n",",",WriteMode.OVERWRITE)
}
