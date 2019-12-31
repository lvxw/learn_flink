package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

object TestApi extends FlinkBaseProgram{
  override def execute(): Unit = {
    val delayOutputTag = OutputTag[String]("late_date")
    sEnv.setParallelism(1)
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    sEnv.enableCheckpointing(Time.minutes(1).toMilliseconds, CheckpointingMode.EXACTLY_ONCE)

//    val result = sEnv.readTextFile(this.getClass.getClassLoader.getResource("1.txt").getPath)
      val result = sEnv.socketTextStream("10.205.11.54", 44444)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(t: String): Long = t.split(",")(1).toLong*1000
      })
//      .assignAscendingTimestamps(_.split(",")(1).toLong*1000)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
//      .allowedLateness(Time.seconds(1))
//        .sideOutputLateData(delayOutputTag)
     /* .apply(new AllWindowFunction[String,String, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[String], out: Collector[String]): Unit = {
          out.collect(s"${window.getStart/1000}-${window.getEnd/1000}:  "+input.mkString(" | "))
        }
      })*/
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          out.collect(s"${context.window.getStart/1000}-${context.window.getEnd/1000}:  "+elements.mkString(" | "))
        }
      })

    result.print()
//    result.getSideOutput(delayOutputTag).print()
    sEnv.execute(jobName)
  }
}
