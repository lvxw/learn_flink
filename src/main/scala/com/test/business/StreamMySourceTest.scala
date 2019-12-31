package com.test.business

import com.test.common.{FlinkBaseProgram, MySensorSource, SensorReading}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamMySourceTest extends FlinkBaseProgram{
  override def execute(): Unit = {
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    sEnv.addSource(new MySensorSource(sensorCount = 1, sleep = 500, isShuffle = true))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000
      })
    /*  .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SensorReading] {
        override def getCurrentWatermark: Watermark = ???

        override def extractTimestamp(t: SensorReading, l: Long): Long = ???
      })
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[SensorReading] {
        override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = ???

        override def extractTimestamp(t: SensorReading, l: Long): Long = ???
      })*/
      .keyBy(_.id)
        .countWindow(5)
        .max(2)
        .print(jobName)


    sEnv.execute(jobName)
  }
}
