package com.test.util

import java.io.File

object FileUtils {
  def deleteDir(dir: File): Unit = {
    if(!dir.exists()){
      return
    }
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
      }
    })
    dir.delete()
  }
}