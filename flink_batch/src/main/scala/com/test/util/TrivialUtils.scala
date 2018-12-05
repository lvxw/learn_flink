package com.test.util

import java.io.{File, IOException}

object TrivialUtils {

  @throws[IOException]
  def deleteOutputDirIfExist(dirFile: File): Unit = {
    if (!dirFile.exists) return
    if (dirFile.isFile) dirFile.delete
    else {
      for (file <- dirFile.listFiles) {
        deleteOutputDirIfExist(file)
      }
      dirFile.delete
    }
  }
}