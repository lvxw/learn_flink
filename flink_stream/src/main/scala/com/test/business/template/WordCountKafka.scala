package com.test.business.template

import java.util.Date

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"zookeeper_host\":\"artemis-02:2181,artemis-03:2181,artemis-04:2181/microlens/artemis/kafka\",
          \"kafka_broker\":\"artemis-02:9092,artemis-03:9092,artemis-04:9092\",
          \"group\":\"WordCountKafka\",
          \"topic\":\"test-flink\",
          \"run_pattern\":\"local\"
        }
  */
object WordCountKafka extends BaseProgram{
  case class WordCount(word:String, num:Long)
  val result = sEnv
    .addSource(getKafkaConsumer())
    .flatMap{x =>
      x.split("\\s")
    }
    .map(WordCount(_,1))
    .keyBy("word")
    .timeWindow(Time.seconds(5),Time.seconds(5))
    .apply{(tuple, window, values, out:Collector[Map[String,Long]]) =>
      val time = sdf.format(new Date())+":"
      val re = values.groupBy(_.word)
        .map{x =>
          var sum = 0L
          val key  = x._1
          val value = x._2
          for(WordCount(k,v) <- value if  k ==key) {
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
}
