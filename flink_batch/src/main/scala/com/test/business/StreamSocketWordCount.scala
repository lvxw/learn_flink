package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.streaming.api.scala._

/**
  main args:
      --host 10.205.12.43
      --port 4444
  */
// nc -lk 4444
object StreamSocketWordCount extends FlinkBaseProgram{
  override def execute(): Unit = {
    val dataSteam = sEnv.socketTextStream(conf.getString("host", "localhost"), conf.getInteger("port", 4444))
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)

    dataSteam.print()

    sEnv.execute(jobName)

  }
}
