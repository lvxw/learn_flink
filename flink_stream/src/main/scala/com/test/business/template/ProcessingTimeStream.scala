package com.test.business.template

import java.text.SimpleDateFormat

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
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
/**
  * kafka 输入日志：
  * r1,k1,20160520093001,001,1
  * r1,k1,20160520093002,001,1
  * r1,k1,20160520093003,001,1
  * r1,k1,20160520093004,001,1
  * r1,k1,20160520093005,001,1
  *
  * r1,k1,20160520093001,001,1
  * r1,k1,20160520093002,001,1
  * r1,k1,20160520093003,001,1
  * r1,k1,20160520093005,001,1
  * r1,k1,20160520093010,001,1
  *
  * r1,k1,20160520093011,001,1
  * r1,k1,20160520093011,001,1
  * r1,k1,20160520093012,001,1
  * r1,k1,20160520093012,001,1
  * r1,k1,20160520093013,001,1
  * r1,k1,20160520093014,001,1
  * r1,k1,20160520093014,001,1
  * r1,k1,20160520093014,001,1
  * r1,k1,20160520093015,001,1
  *
  * r1,k1,20160520093016,001,1
  * r1,k1,20160520093016,001,1
  * r1,k1,20160520093020,001,1
  */
object ProcessingTimeStream extends BaseProgram{

  sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  val result = sEnv.addSource(getKafkaConsumer(consumeType = consumeTypeMap.get(2).get))
    .map{lineStr =>
      val fieldArr = lineStr.split(",")

      val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")

      val key = fieldArr(1)
      val volume = fieldArr(4).toLong

      val eventTimeStr = fieldArr(2)+fieldArr(3)
      val evenTime = sdf.parse(eventTimeStr).getTime
      (evenTime,key,volume)
    }
    .keyBy(_._2)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .apply{ (str:String, window:TimeWindow, iterable: Iterable[(Long,String,Long)], collector:Collector[Map[String,String]]) =>
      val keyStr = window.maxTimestamp()+""
      val result = iterable.map(x => (keyStr,x._2,x._3)).groupBy(_._1)

      val dd = result.flatMap{x =>
        val it = x._2.map(x => (x._2,x._3))
        val gr = it.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum))
        val its = gr.map(x => x._1+"->"+x._2)
        val kk = for(x <- its) yield (keyStr,x)
        kk
      }
      collector.collect(dd)
    }.flatMap(x => x)

  result.addSink(redisSink)
}
