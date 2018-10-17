package com.test.business.template

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 运行参数：
  *   --port 9000
  *
  * 执行命令
  */
object wordCountStream extends App {
  case class WordCount(word:String, count:Long)

  val port = ParameterTool.fromArgs(args).getInt("port")

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  private val stream: DataStream[String] = env.socketTextStream("localhost", port)

  val result = stream.flatMap(_.split("\\s"))
    .map(WordCount(_,1))
    .keyBy("word")
    .timeWindow(Time.seconds(5),Time.seconds(5))
    .sum("count")

  result.setParallelism(1).print()
  env.execute("WordCount")
}