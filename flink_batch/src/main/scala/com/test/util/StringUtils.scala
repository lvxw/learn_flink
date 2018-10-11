package com.test.util

import scala.util.parsing.json.JSON

object StringUtils {

  /**
    * 通过模式匹配解析json类型参数
    * @param json
    * @return
    */
  def jsonStrToMap(args:Array[String]):Any ={
    val jsonObj = JSON.parseFull(args.mkString(""))
    jsonObj match {
      case Some(map: Map[String, String]) => map
      case _ => None
    }
  }
}
