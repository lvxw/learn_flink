package com.test.common

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random
import scala.collection.JavaConverters._


case class SensorReading(id: String, timestamp: Long, temperature: Double)
class MySensorSource(sensorCount:Int=10 ,sleep:Long=100, isShuffle:Boolean = false) extends SourceFunction[SensorReading]{
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()
    val arrayList = new util.ArrayList[Int]()
    1.to(sensorCount).foreach(arrayList.add(_))
    while(running){
      if(isShuffle) Collections.shuffle(arrayList)
      arrayList.asScala.map(el => {
        (s"sensor_${el}", 65 + random.nextGaussian() * 20)
      }).foreach(t =>{
        sourceContext.collect(SensorReading(t._1, System.currentTimeMillis(), t._2 + random.nextGaussian()))
        Thread.sleep(sleep)
      })
//      println("-----------------------------------------------------------------------------------------")
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}


class AA extends RichSourceFunction {
  override def run(sourceContext: SourceFunction.SourceContext[Nothing]): Unit = ???

  override def cancel(): Unit = ???
}
