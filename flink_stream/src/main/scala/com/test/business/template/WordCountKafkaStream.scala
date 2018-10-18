package com.test.business.template

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

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
    .map{x =>
      if(x.isEmpty){
        ("\"\"",1)
      }else{
        (x,1)
      }
    }
    .keyBy(0)
    .timeWindow(Time.seconds(5),Time.seconds(5))
    .apply{(tuple, window, values, out:Collector[Map[String,Int]]) =>
      val time = window.maxTimestamp()+":"
      val re = values.groupBy(_._1)
        .map{x =>
          var sum = 0
          val key  = x._1
          val value = x._2
          for((k,v) <- value if  k ==key) {
            sum += v
          }
          (time+key,sum)
        }
      out.collect(re)
    }
    .flatMap(x => x)
    .map{x =>
      val kv = x._1.split(":")
      (kv(0),kv(1)+"->"+x._2)
    }

  result.addSink(redisSink)
}
