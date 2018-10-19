package com.test.business.template

import com.test.common.BaseProgram
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"topic\":\"test-flink\",
          \"run_pattern\":\"local\",
          \"checkpoint_interval\":\"5000\",
          \"checkpoint_mode\":\"EXACTLY_ONCE\"
        }
  */
object WordCountKafkaStream2 extends BaseProgram{

  val result = sEnv
    .addSource(getKafkaConsumer())
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
    .window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](2)))
    .sum(1)

  result.print()

}
