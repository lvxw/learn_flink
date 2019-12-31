package com.test.business

import com.test.common.FlinkBaseProgram
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  main args:
      --zookeeper.connect master:2181,slave1:2181,slave2:2181
      --bootstrap.servers master:9092,slave1:9092,slave2:9092
      --group.id wordCount
      --topic test
  */

/**
  test data:
    s1,1577415705,45.6
    s1,1577415706,45.3
    s1,1577415707,45.9
    s1,1577415708,45.2
*/
object StreamKafkaWordCount extends FlinkBaseProgram{
  override def execute(): Unit = {
//    testTOne()
    testTwo()
  }

  def testTwo():Unit = {
    sEnv.setParallelism(1)
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val result = sEnv.socketTextStream("10.205.11.54",44444)
      .map(lineStr =>{
        val tmpArr = lineStr.split(",")
        (tmpArr(0), tmpArr(1).toLong, tmpArr(2).toDouble)
      })
      .assignAscendingTimestamps(_._2*1000)
      .keyBy(_._1)
      .process(new KeyedProcessFunction[String,(String, Long, Double), (String, Long, Double)] {
        override def processElement(i: (String, Long, Double), context: KeyedProcessFunction[String, (String, Long, Double), (String, Long, Double)]#Context, collector: Collector[(String, Long, Double)]): Unit = {
          if(i._3 > 45.3){
            collector.collect(i)
          }
        }
      })

    result
//      .reduce((el1, el2) => (el1._1, el1._2, el1._3.max(el2._3)))
//      .print(jobName)

    sEnv.execute(jobName)
  }

  def testTOne(): Unit ={
    val dataStream = sEnv.addSource(getFlinkKafkaConsumer())
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5),Time.seconds(5))
      .reduce((e1,e2) =>(null, e1._2+e2._2))
      .map(_._2)

    dataStream.print()
    sEnv.execute(jobName)
  }
}
