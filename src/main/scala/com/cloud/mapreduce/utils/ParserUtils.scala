package com.cloud.mapreduce.utils

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object ParserUtils {

  val logger: Logger = LoggerFactory.getLogger(ParserUtils.getClass)

  /**
   * Utility function to sort ArrayBuffer in ascending order
   *
   * @param xs
   * @tparam T
   * @return
   */
  def ascendingSort[T <% Ordered[T]](xs: ArrayBuffer[T]) = xs.sortWith(_ < _)

  /**
   * Pre format xml by replacing the xml specific tags with their encodings
   *
   * @param document
   * @return
   */
  def preFormatXml(document: String): String = {
    document.replaceAll("\n", "").replaceAll("&", "&amp;").replaceAll("'", "&apos;").replaceAll("^(.+)(<)([^>/a-zA-z_]{1}[^>]*)(>)(.+)$", "$1&lt;$3&gt;$5")
  }

  /**
   * Format the csv into array of rows to make it easy to write to HDFS
   *
   * @param header
   * @param columns
   * @return
   */
  def formatCsv(header: Array[String], columns: Array[ArrayBuffer[String]]): ArrayBuffer[String] = {
    val output = new ArrayBuffer[String]

    val headerString = new StringBuilder
    header.foreach(each => headerString.append(each).append(","))
    headerString.replace(headerString.size - 1, headerString.size, "\n")
    output.addOne(headerString.toString())

    val columnSize = columns(0).size
    for (index <- 0 until columnSize) {
      val eachRow = new StringBuilder
      columns.foreach(row => {
        eachRow.append(row(index)).append(",")
      })
      eachRow.replace(eachRow.size - 1, eachRow.size, "\n")
      output.addOne(eachRow.toString)
    }
    output
  }
}