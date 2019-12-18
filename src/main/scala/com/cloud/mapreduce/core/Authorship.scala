package com.cloud.mapreduce.core

import java.io.{ByteArrayInputStream, IOException}
import java.lang.Iterable

import com.cloud.mapreduce.config.MapReduceConfig
import com.cloud.mapreduce.paser.XmlInputFormat
import com.cloud.mapreduce.utils.{ParserUtils, ReadWriteUtils}
import com.ctc.wstx.exc.WstxParsingException
import javax.xml.stream.XMLInputFactory
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Authorship {

  final val logger: Logger = LoggerFactory.getLogger(Authorship.getClass)

  class Map extends Mapper[LongWritable, Text, Text, DoubleWritable] {

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context): Unit = {
      val document = value.toString
      try {
        val formattedDocument = ParserUtils.preFormatXml(document)
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(formattedDocument.getBytes))
        val authors = new ArrayBuffer[String]()
        var isAuthor = false
        while (reader.hasNext) {
          try {
            reader.next
            if (isAuthor) {
              authors.addOne(reader.getText)
              isAuthor = false
            }

            if (reader.isStartElement) {
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                isAuthor = true
              }
            }
          } catch {
            case wex: WstxParsingException =>
              logger.error("Some exception Occurred while parsing XML", wex)
            case iex: IllegalStateException =>
              logger.error("Some exception Occurred while parsing XML", iex)
          }
        }
        reader.close()
        val noOfAuthors = authors.size
        val outValue: DoubleWritable = new DoubleWritable("%.2f".format(1d / noOfAuthors).toDouble)
        if (noOfAuthors != 0) {
          authors.foreach(author => {
            context.write(new Text(author), outValue)
          })
        }
      } catch {
        case e: Exception =>
          logger.error("Some exception Occurred while parsing XML", e)
          throw new IOException(e)
      }
    }
  }

  class Reduce extends Reducer[Text, DoubleWritable, Text, DoubleWritable] {

    private val result = new DoubleWritable

    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: Iterable[DoubleWritable], context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
      var sum = 0d
      val scalaValues = values.asScala
      var valueCount = 0
      scalaValues.foreach(value => {
        sum += value.get
        valueCount += 1
      })
      result.set("%.2f".format(sum / valueCount).toDouble)
      context.write(key, result)
    }
  }

  def getFinalCSV(input: String): Array[ArrayBuffer[String]] = {
    val lines: String = ReadWriteUtils.readHdfsPartFiles(input)
    val line: Array[String] = lines.split("\n")
    val splitLines: Array[Array[String]] = line.filter(each => !StringUtils.isBlank(each)).map(each => each.split(","))
    val size = splitLines.size
    val countReq = Integer.parseInt(MapReduceConfig.configObj.config.getString("AuthorshipCount"))
    val topAuthors = new ArrayBuffer[String]()
    for (index <- 0 until (countReq)) {
      topAuthors.addOne(splitLines(index)(0))
    }

    val leastAuthors = new ArrayBuffer[String]()
    for (index <- size - 1 to (size - countReq) by -1) {
      leastAuthors.addOne(splitLines(index)(0))
    }
    Array(topAuthors, leastAuthors)
  }


  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "MapReduceJob4"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(Authorship.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[DoubleWritable])
    job.setMapperClass(classOf[Authorship.Map])
    job.setReducerClass(classOf[Authorship.Reduce])
    val outputDir = output.replace("(jobName)", jobName)
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Top 100 Authors who publish with most Co-authors", "Top 100 Authors who publish with least Co-authors")
    if (job.waitForCompletion(true)) {
      SortingJob.runJob(output.replace("(jobName)", jobName), output, jobName)
      ReadWriteUtils.writeToHdfs(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(output.replace("(jobName)", "Sorted" + jobName))))
    } else System.exit(1)
  }
}