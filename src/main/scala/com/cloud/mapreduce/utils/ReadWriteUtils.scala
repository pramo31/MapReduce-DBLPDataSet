package com.cloud.mapreduce.utils

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

object ReadWriteUtils {

  final val logger: Logger = LoggerFactory.getLogger(ReadWriteUtils.getClass)

  /**
   * Utility function to write lines to hdfs file
   *
   * @param output
   * @param csvFile
   * @param content
   * @throws
   */
  @throws[IOException]
  def writeToHdfs(output: String, csvFile: String, content: ArrayBuffer[String]): Unit = {
    val fs = FileSystem.get(new Configuration())
    val hdfsPath = new Path(output + "/" + csvFile + ".csv")
    val outputStream = fs.create(hdfsPath)
    content.foreach(row => outputStream.writeBytes(row))
    outputStream.close()
  }

  /**
   * Utility function to read part files from hdfs function
   *
   * @param input
   * @throws
   * @return
   */
  @throws[IOException]
  def readHdfsPartFiles(input: String): String = {
    val fs = FileSystem.get(new Configuration())
    val status: Array[FileStatus] = fs.listStatus(new Path(input))
    val files = status.filter(f => f.getPath.getName.startsWith("part-"))
    val lines = new StringBuilder
    for (k <- files.indices) {
      val stream = fs.open(files(k).getPath)
      val readLines: BufferedSource = Source.fromInputStream(stream)
      lines.append(readLines.mkString)
      lines.append("\n")
    }
    fs.close()
    lines.toString()
  }
}