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
object WordCount extends BaseProgram{

  val text = env.readTextFile(inputDir)
  val result = text.flatMap ( _.split(","))
    .map ((_, 1))
      .groupBy(0)
      .sum(1)
  result.print()
  result.writeAsCsv(outputDir,"\n",",",WriteMode.OVERWRITE)
}
