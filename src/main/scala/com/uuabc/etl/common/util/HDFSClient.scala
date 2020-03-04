package com.uuabc.etl.common.util

import java.net.URI
import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream, FSDataOutputStream}
import scala.collection.mutable.ArrayBuffer
import com.uuabc.etl.common.Logging

object HDFSClient extends Logging {

  private lazy val fs = {
    val conf = new Configuration()
    FileSystem.get(conf)
  }

  def readFile(path: String): String = {
    var input: FSDataInputStream = null
    var br: BufferedReader = null
    val result = new ArrayBuffer[String]()

    try {
      val target = new Path(path)
      input = fs.open(target)
      br = new BufferedReader(new InputStreamReader(input))
      var line = br.readLine()
      while (line != null) {
        result += line
        line = br.readLine()
      }
    } finally {
      if (input != null) input.close()
      if (br != null) br.close()
    }

    result.mkString("\n")
  }

  def writeFile(path: String, data: Iterator[String]) = {
    var output: FSDataOutputStream = null

    try {
      val target = new Path(path)
      // if (!fs.exists(target)) {
      //   output = fs.create(target, true)
      // }
      output = fs.create(target, true)
      data.foreach(d => output.writeBytes(d + "\n"))
    } catch {
      case e: Exception =>
        logError("write file error ")
    } finally {
      if (output != null) output.close()
    }
  }

  def deleteFile(path: String): Unit = {
    val target = new Path(path)
    if (fs.exists(target))
      fs.delete(target, true)
  }
}
