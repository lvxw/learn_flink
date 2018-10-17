package com.test.util

import scala.io.Source
import scala.util.parsing.json.JSON

object ParamUtils{

  /**
    * 读取classpath json格式的配置文件，并转成对象
    * @param path
    * @return
    */
  def getClassPathFileContent(path:String):Map[String,Any] ={
    val content = Source.fromURL(this.getClass.getClassLoader.getResource(path)).mkString
    jsonStrToMap(content).asInstanceOf[Map[String,Any]]
  }

  /**
    * json字符串转map
    * @param args
    * @return
    */
  def jsonStrToMap(jsonStr:String):Any ={
    val jsonObj = JSON.parseFull(jsonStr)
    jsonObj match {
      case Some(map: Map[String, String]) => map
      case _ => None
    }
  }
}
