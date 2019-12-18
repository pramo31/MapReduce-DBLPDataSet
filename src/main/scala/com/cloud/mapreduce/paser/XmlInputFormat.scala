package com.cloud.mapreduce.paser

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

class XmlInputFormat extends TextInputFormat {

  final val logger: Logger = LoggerFactory.getLogger(XmlInputFormat.super.getClass)

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = new XmlRecordReader

  /**
   * XMLRecordReader class to read through a given xml document to output
   * xml blocks as records as specified by the start tag list and end tag list
   */
  class XmlRecordReader extends RecordReader[LongWritable, Text] {

    private val startTagList = new ArrayBuffer[Array[Byte]]
    private val endTagList = new ArrayBuffer[Array[Byte]]
    private var start = 0L
    private var end = 0L
    private var fsin: FSDataInputStream = null
    private val buffer: DataOutputBuffer = new DataOutputBuffer
    private val key: LongWritable = new LongWritable
    private val value: Text = new Text

    /**
     *
     * @param split
     * @param context
     * @throws
     * @throws
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {

      // Tags Using which we have to split the xml
      val tagPair: HashMap[String, String] = HashMap(("<phdthesis ", "</phdthesis>"), ("<www ", "</www>"), ("<mastersthesis ", "</mastersthesis>"), ("<article ", "</article>"), ("<inproceedings ", "</inproceedings>"), ("<proceedings ", "</proceedings>"), ("<book ", "</book>"), ("<incollection ", "</incollection>"))
      val conf = context.getConfiguration
      tagPair.foreach(x => {
        startTagList.addOne(x._1.getBytes("utf-8"))
        endTagList.addOne(x._2.getBytes("utf-8"))
      })

      val fileSplit = split.asInstanceOf[FileSplit]
      logger.debug("Start and End of Split : " + start + " , " + end)
      start = fileSplit.getStart
      end = start + fileSplit.getLength
      val file = fileSplit.getPath
      logger.debug("Split Details (Start, End, Path) : " + start + " , " + end + " , " + file)
      val fs = file.getFileSystem(conf)
      fsin = fs.open(fileSplit.getPath)
      fsin.seek(start)
    }

    /**
     * Utility function to fetch the next matching xml tags
     *
     * @throws
     * @throws
     * @return
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def nextKeyValue: Boolean = {
      if (fsin.getPos < end) {
        val matchedIndex = readUntilMatch(startTagList, false)
        if (matchedIndex != -1) try {
          buffer.write(startTagList(matchedIndex))
          if (readUntilMatch(endTagList(matchedIndex), true)) {
            key.set(fsin.getPos)
            value.set(buffer.getData, 0, buffer.getLength)
            return true
          }
        } finally buffer.reset
      }
      false
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentKey: LongWritable = key

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentValue: Text = value

    @throws[IOException]
    override def close(): Unit = {
      fsin.close()
    }

    @throws[IOException]
    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    /**
     * Find the start tag matching the preset list of tags
     *
     * @param matcher
     * @param withinBlock
     * @throws
     * @return
     */
    @throws[IOException]
    def readUntilMatch(matcher: ArrayBuffer[Array[Byte]], withinBlock: Boolean): Int = {
      val defaultMatchedIndices = new ArrayBuffer[Integer]
      for (j <- 0 to startTagList.size - 1) {
        defaultMatchedIndices.addOne(j)
      }
      var matchedIndices = defaultMatchedIndices
      var i = 0
      while (true) {
        val b = fsin.read()
        // end of file:
        if (b == -1) return -1
        // save to buffer:
        if (withinBlock) buffer.write(b)
        val tempMatchedIndices = new ArrayBuffer[Integer]


        var iFlag = false
        // Check if we're matching any of the tags
        matchedIndices.foreach(index => {
          if (b == startTagList(index)(i)) {
            iFlag = true
            tempMatchedIndices.addOne(index)
          }
        })

        if (iFlag) {
          i += 1
          // Return the index when finish tag is found
          tempMatchedIndices.foreach(matchIndex => {
            if (i >= startTagList(matchIndex).length) {
              return matchIndex
            }
          })
        }
        else i = 0
        if (tempMatchedIndices.size == 0) matchedIndices = defaultMatchedIndices
        else matchedIndices = tempMatchedIndices
        // See if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos >= end) return -1
      }
      return -1
    }

    /**
     * Matcher function to add all characters after finding start tag till the end tag is reached
     *
     * @param matcher
     * @param withinBlock
     * @throws
     * @return
     */
    @throws[IOException]
    def readUntilMatch(matcher: Array[Byte], withinBlock: Boolean): Boolean = {
      var i = 0
      while (true) {
        val b = fsin.read()
        if (b == -1) return false
        if (withinBlock) buffer.write(b)
        // check if we're matching:
        if (b == matcher(i)) {
          i += 1
          if (i >= matcher.length) return true
        }
        else i = 0
        if (!withinBlock && i == 0 && fsin.getPos >= end) return false
      }
      true
    }
  }
}