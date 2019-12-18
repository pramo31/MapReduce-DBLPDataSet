package com.cloud.mapreduce.core

import java.io.{ByteArrayInputStream, IOException}
import java.util.concurrent.atomic.AtomicInteger
import java.lang.Iterable

import scala.collection.JavaConverters._
import com.cloud.mapreduce.paser.XmlInputFormat
import com.cloud.mapreduce.utils.{ParserUtils, ReadWriteUtils}
import com.ctc.wstx.exc.WstxParsingException
import javax.xml.stream.XMLInputFactory
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object MeanMedianStatistics {
  val logger: Logger = LoggerFactory.getLogger(MeanMedianStatistics.getClass)

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val document = value.toString
      try {
        val formattedDocument = ParserUtils.preFormatXml(document)
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(formattedDocument.getBytes))
        val authorCount = new AtomicInteger(0)
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
                authorCount.incrementAndGet()
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
        val noOfAuthore = authors.size
        if (noOfAuthore != 0) {
          val noOfCoAuthors = noOfAuthore - 1
          authors.foreach(author => {
            context.write(new Text(author), new IntWritable(noOfCoAuthors))
          })
        }
      } catch {
        case e: Exception =>
          logger.error("Some exception Occurred while parsing XML", e)
          throw new Exception(e)
      }
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, Text] {

    private val result = new IntWritable

    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      val sum = new AtomicInteger(0)
      val lstNoOfCoAuthors = new ArrayBuffer[Int]()
      val scalaValues = values.asScala
      scalaValues.foreach(value => {
        val noOfCoAuthors = value.get
        sum.addAndGet(noOfCoAuthors)
        lstNoOfCoAuthors.addOne(noOfCoAuthors)
      })
      val sorted = ParserUtils.ascendingSort(lstNoOfCoAuthors)
      val arraySize = lstNoOfCoAuthors.size
      val median = lstNoOfCoAuthors(arraySize / 2)
      val mean = sum.get / arraySize
      val min = sorted(0)
      val max = sorted(arraySize - 1)
      val output = mean + "\t" + median + "\t" + min + "\t" + max
      context.write(new Text(key), new Text(output))
    }
  }

  def getFinalCSV(input: String): Array[ArrayBuffer[String]] = {
    val lines: String = ReadWriteUtils.readHdfsPartFiles(input)
    val line: Array[String] = lines.split("\n")
    val splitLines: Array[Array[String]] = line.filter(each => !StringUtils.isBlank(each)).map(each => each.split(","))
    val authors = new ArrayBuffer[String]
    val median = new ArrayBuffer[String]
    val mean = new ArrayBuffer[String]
    val min = new ArrayBuffer[String]
    val max = new ArrayBuffer[String]
    splitLines.foreach(each => {
      val author = each(0)
      val stats = each(1).split("\t")
      authors.addOne(author)
      mean.addOne(stats(0))
      median.addOne(stats(1))
      min.addOne(stats(2))
      max.addOne(stats(3))
    })
    Array(authors, mean, median, min, max)
  }

  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "MapReduceJob5"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(MeanMedianStatistics.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[MeanMedianStatistics.Map])
    job.setReducerClass(classOf[MeanMedianStatistics.Reduce])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output.replace("(jobName)", jobName)
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Author", "Mean", "Median", "Minimum", "Maximum")
    if (job.waitForCompletion(true)) {
      ReadWriteUtils.writeToHdfs(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(outputDir)))
    } else System.exit(1)
  }
}