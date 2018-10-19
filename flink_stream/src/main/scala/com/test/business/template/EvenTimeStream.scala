package com.test.business.template

import com.test.common.BaseProgram
import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

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
object EvenTimeStream extends BaseProgram{

  sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val result = sEnv.addSource(getKafkaConsumer(consumeType = consumeTypeMap.get(2).get))
    .map(new EventTimeFunction)
    .assignAscendingTimestamps(_._1)
    .keyBy(_._2)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(2)
    .map(x => (x._1.toString,s"${x._2}->${x._3}"))

  result.addSink(redisSink)


  case class Transaction(
      rowNum:String,
      key:String,
      ymdhms:String,
      ns:String,
      volume:Long
  )

  class EventTimeFunction extends MapFunction[String,(Long,String,Long)]{
    override def map(lineStr: String): (Long,String,Long) = {
      val fieldArr = lineStr.split(",")
      val transaction = Transaction(
        fieldArr(0),
        fieldArr(1),
        fieldArr(2),
        fieldArr(3),
        fieldArr(4).toLong
      )

      val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")

      val key = transaction.key
      val volume = transaction.volume

      val eventTimeStr = transaction.ymdhms+transaction.ns
      val evenTime = sdf.parse(eventTimeStr).getTime
      (evenTime,key,volume)
    }
  }
}
