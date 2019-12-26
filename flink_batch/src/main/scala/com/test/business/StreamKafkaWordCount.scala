package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  main args:
      --zookeeper.connect master:2181,slave1:2181,slave2:2181
      --bootstrap.servers master:9092,slave1:9092,slave2:9092
      --group.id wordCount
      --topic test
  */
object StreamKafkaWordCount extends FlinkBaseProgram{
  override def execute(): Unit = {
//    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = sEnv.addSource(getFlinkKafkaConsumer())
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5),Time.seconds(5))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(20)))
      .reduce((e1,e2) =>(null, e1._2+e2._2))
      .map(_._2)

      dataStream.print()

    sEnv.execute(jobName)

  }
}
