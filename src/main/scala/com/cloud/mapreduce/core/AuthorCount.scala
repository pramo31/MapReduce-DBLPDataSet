package com.cloud.mapreduce.core

import java.io.{ByteArrayInputStream, IOException}
import java.lang.Iterable
import java.util.concurrent.atomic.AtomicInteger

import com.cloud.mapreduce.config.MapReduceConfig
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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AuthorCount {

  val logger: Logger = LoggerFactory.getLogger(AuthorCount.getClass)

  class Map extends Mapper[LongWritable, Text, IntWritable, IntWritable] {

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, IntWritable]#Context): Unit = {
      val document = value.toString
      try {
        val formattedDocument = ParserUtils.preFormatXml(document)
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(formattedDocument.getBytes))
        val outKey = new AtomicInteger(0)
        val outValue = new IntWritable(1)
        while (reader.hasNext) {
          try {
            reader.next
            if (reader.isStartElement) {
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                outKey.incrementAndGet()
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
        if (outKey.get != 0) context.write(new IntWritable(outKey.get), outValue)
      } catch {
        case e: Exception =>
          logger.error("Some exception Occurred while parsing XML", e)
          throw new Exception(e)
      }
    }
  }

  class Reduce extends Reducer[IntWritable, IntWritable, IntWritable, IntWritable] {

    private val result = new IntWritable

    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: IntWritable, values: Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
      val sum = new AtomicInteger(0)
      val scalaValues = values.asScala
      scalaValues.foreach(value => sum.addAndGet(value.get))
      result.set(sum.get())
      context.write(key, result)
    }
  }

  def getFinalCSV(input: String): Array[ArrayBuffer[String]] = {
    val lines: String = ReadWriteUtils.readHdfsPartFiles(input)
    val line: Array[String] = lines.split("\n")
    val splitLines: Array[Array[String]] = line.filter(each => !StringUtils.isBlank(each)).map(each => each.split(","))
    val authorCount = new mutable.HashMap[Int, Int]()
    val highestBin = new AtomicInteger()

    splitLines.foreach(each => {
      val bin = Integer.parseInt(each(0).trim)
      authorCount.put(bin, Integer.parseInt(each(1).trim))
      if (bin > highestBin.get()) {
        highestBin.set(bin)
      }
    })
    val finalBinKey = new ArrayBuffer[String]
    val finalBinValue = new ArrayBuffer[String]

    val bucketArray: Array[String] = MapReduceConfig.configObj.config.getString("AuthorCountBin").split(",")
    bucketArray.foreach(bucket => {
      if (bucket.contains("end")) {
        val bucketRange = bucket.split("-")
        val start = new AtomicInteger(Integer.parseInt(bucketRange(0)))
        val bin = new mutable.StringBuilder(start + "-")
        val sum = new AtomicInteger(0)
        while (start.get <= highestBin.get) {
          if (authorCount.contains(start.get)) {
            sum.set(sum.get + authorCount.get(start.get).get)
          }
          start.incrementAndGet()
        }
        val end = start.decrementAndGet()
        bin.append(end.toString)
        finalBinKey.addOne(bin.toString())
        finalBinValue.addOne(sum.get.toString)
      } else if (bucket.contains("-")) {
        val bucketRange = bucket.split("-")
        val start = Integer.parseInt(bucketRange(0))
        val end = Integer.parseInt(bucketRange(1))
        val sum = new AtomicInteger(0)
        for (range <- start to end) {
          if (authorCount.contains(range)) {
            sum.set(sum.get + authorCount.get(range).get)
          }
        }
        finalBinKey.addOne(bucket)
        finalBinValue.addOne(sum.get.toString)
      } else {
        val bin = Integer.parseInt(bucket)
        if (authorCount.contains(bin)) {
          finalBinKey.addOne(bucket)
          finalBinValue.addOne(authorCount.get(bin).get.toString)
        } else {
          finalBinKey.addOne(bucket.toString())
          finalBinValue.addOne(0.toString)
        }
      }
    })
    Array(finalBinKey, finalBinValue)
  }

  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "MapReduceJob1"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(AuthorCount.getClass)
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[AuthorCount.Map])
    job.setReducerClass(classOf[AuthorCount.Reduce])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    val outputDir = output.replace("(jobName)", jobName)
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Co-Author Bin", "Number Of Co-Authors")
    if (job.waitForCompletion(true)) {
      ReadWriteUtils.writeToHdfs(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(outputDir)))
    } else System.exit(1)
  }
}
