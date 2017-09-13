package com.hcdlearning.etl.common.util

import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream, FSDataOutputStream}

class HDFSClient extends Logging {

  private def withHDFS((f: FileSystem => Unit): Unit = {
    var fs: FileSystem = null

    try {
      val conf = new Configuration()
      fs = FileSystem.get(conf)
      f(fs)
    }
    finally {
      try {
        if (fs != null) fs.close()
      } catch {
        case _: Exception => logError(_.getStackTrace)
      }
    }
  }

  def readFile(path: String): String = {
    withHDFS { fs =>
      var input: FSDataOutputStream = null
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
  }

  def writeFile(path: String, data: Iterator[String]) = {
    withHDFS { fs =>
      var output: FSDataOutputStream = null

      try {
        val target = new Path(path)
        // if (!fs.exists(target)) {
        //   output = fs.create(target, true)
        // }
        output = fs.create(target)
        data.foreach(d => output.writeBytes(d + "\n"))
      } catch {
        case e: Exception =>
          logError("write file error ")
      } finally {
        if (output != null) output.close()
      }
    }
  }

  def deleteFile(path: String): Boolean = {
    withHDFS { fs =>
      val target = new Path(path)
      if (fs.exists(path))
        fs.delete(path, true)
    }
  }
}