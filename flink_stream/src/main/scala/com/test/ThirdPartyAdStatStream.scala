package com.test

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"topic\":\"funx_pro_exchange\",
          \"run_pattern\":\"local\"
        }
  */
object ThirdPartyAdStatStream extends BaseProgram{
  case class Record(val timestamp:String, val despCode:String,val code:String)
  val prefix = "microlens.hermes.funxpro.exchange.endpoints."
  val normalPath = ".status.gte2.counters.request.realtime.count"
  val timeoutPath = ".status.code_2101.counters.request.realtime.count"
  val normalMark = "1"
  val timeoutMark = "2"
  val otherAndExceptMark = "3"

  val re = sEnv.addSource(getKafkaConsumer())
    .map{lineStr =>
      val fieldArr = lineStr.split(",")
      if(fieldArr.length >= 14){
        val code = fieldArr(13).toInt
        val timestamp = fieldArr(1)
        val dspCode = fieldArr(10)
        val markByCode = if(code==0 || code>2000) normalMark else if(code==2101) timeoutMark else otherAndExceptMark
        (s"${dspCode},${timestamp},${markByCode}",1)
      }else{
        (s",${otherAndExceptMark}",1)
      }

    }
    .keyBy(0)
    .timeWindow(Time.seconds(1),Time.seconds(1))
    .apply{(tuple, window, values, out:Collector[List[(String,String,Long)]]) =>
      val re = values.filter(x => !x._1.endsWith(s",${otherAndExceptMark}")).groupBy(_._1)
      .map{x =>
        val fieldsArr = x._1.split(",")
        val dspCode = fieldsArr(0)
        val timestamp = fieldsArr(1).toLong
        val markByCode  = fieldsArr(2)

        val path  = if(markByCode == normalMark) s"${prefix}${dspCode}${normalPath}" else s"${prefix}${dspCode}${timeoutPath}"
        val value = x._2.map(_._2).sum.toString

        (path,value,timestamp)
      }.toList
      out.collect(re)
    }.flatMap(x=>x)

  re.addSink(graphiteSink)
}
