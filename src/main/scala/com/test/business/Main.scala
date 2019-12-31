package com.test.business

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

object Main extends App {
  val inclusive = 1.to(3).toSet.toList.asJava
  Collections.shuffle(inclusive)

}
