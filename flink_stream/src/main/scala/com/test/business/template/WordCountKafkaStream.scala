package com.test.business.template

import java.text.SimpleDateFormat
import java.util.Date

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"topic\":\"test-flink\",
          \"run_pattern\":\"local\"
        }
  */
object WordCountKafkaStream extends BaseProgram{

  val result = sEnv
    .addSource(kafkaConsumer)
    .flatMap{x =>
      x.split("\\s")
    }
    .map((_,1))
    .keyBy(0)
    .timeWindow(Time.seconds(5),Time.seconds(5))
    .sum(1)
    .map(x => (new SimpleDateFormat("yyyyMMddHHmm").format(new Date()),x._1+" -> "+x._2+""))

  result.addSink(redisSink)
}
