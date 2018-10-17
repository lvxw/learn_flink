package com.test.common

import com.codahale.metrics.graphite.PickledGraphite
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class GraphiteSink[IN](val host:String,port:Int,batchSize:Int) extends RichSinkFunction[IN]{

  private var graphite: PickledGraphite =_


  @throws[Exception]
  override def invoke(input: IN): Unit = {
    val tuple = input.asInstanceOf[(String,String)]
    if (graphite.isConnected) {
      println("the graphite is connected.")
    }

    println("键是："+tuple._1,"值是:"+tuple._2)
  }

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    graphite = new PickledGraphite(host,port,batchSize)
    graphite.connect()
  }

  @throws[Exception]
  override def close(): Unit = {
    graphite.close()
  }
}
