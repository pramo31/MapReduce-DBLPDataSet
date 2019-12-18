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

object PublicationYearStratification {

  val logger: Logger = LoggerFactory.getLogger(PublicationYearStratification.getClass)

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {

    /**
     * Map function which maps "author count-year' to value 1
     *
     * @param key
     * @param value
     * @param context
     * @throws
     * @throws
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val document = value.toString
      try {
        val formattedDocument = ParserUtils.preFormatXml(document)
        val reader = XMLInputFactory.newInstance.createXMLStreamReader(new ByteArrayInputStream(formattedDocument.getBytes))
        var authorCount = 0
        var year = ""
        val outValue = new IntWritable(1)
        var isTextual = false
        while (reader.hasNext) {
          try {
            reader.next
            if (isTextual) {
              year = reader.getText
              isTextual = false
            }

            if (reader.isStartElement) {
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                authorCount += 1
              }
              if (currentElement eq "year") {
                isTextual = true
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
        if (authorCount > 0 && year != "") {
          val output = authorCount + "-" + year
          context.write(new Text(output), outValue)
        }
      } catch {
        case e: Exception =>
          logger.error("Some exception Occurred while parsing XML", e)
          throw new IOException(e)
      }
    }
  }


  class Reduce extends Reducer[Text, IntWritable, Text, IntWritable] {

    private val result = new IntWritable

    /**
     * Reduce function does a simple count of the values which will be count of the no of keys or 'author count-year' occurrings
     *
     * @param key
     * @param values
     * @param context
     * @throws
     * @throws
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      val scalaValues = values.asScala
      scalaValues.foreach(value => sum += value.get)
      result.set(sum)
      context.write(key, result)
    }
  }

  /**
   *
   * @param input
   * @return
   */
  def getFinalCSV(input: String): Array[ArrayBuffer[String]] = {
    val lines: String = ReadWriteUtils.readHdfsPartFiles(input)
    val line: Array[String] = lines.split("\n")
    val splitLines: Array[Array[String]] = line.filter(each => !StringUtils.isBlank(each)).map(each => each.split(","))
    val authorCount = new mutable.HashMap[Int, mutable.HashMap[Int, Int]]()

    val highestBin = new AtomicInteger(0)

    splitLines.foreach(each => {
      val compositeKey = each(0).split("-")
      val bin = Integer.parseInt(compositeKey(0))
      val year = Integer.parseInt(compositeKey(1))
      val count = Integer.parseInt(each(1))
      if (authorCount.contains(bin)) {
        authorCount.get(bin).get.put(year, count)
      }
      else {
        val stratify = new mutable.HashMap[Int, Int]()
        stratify.put(year, count)
        authorCount.put(bin, stratify)
      }

      if (bin > highestBin.get()) {
        highestBin.set(bin)
      }
    })

    val finalBinKey = new ArrayBuffer[String]
    val finalBinYearKey = new ArrayBuffer[String]
    val finalBinValue = new ArrayBuffer[String]

    val yearBucket: Array[String] = MapReduceConfig.configObj.config.getString("YearStratificationBin").split(",")
    val tempMap = new mutable.HashMap[Int, mutable.HashMap[String, Int]]()
    authorCount.keys.foreach(key => {
      val stratifiedByYears = authorCount.get(key).get
      yearBucket.foreach(yearBin => {
        val yearRange = yearBin.split("-")
        val start = Integer.parseInt(yearRange(0))
        val end = Integer.parseInt(yearRange(1))
        val sum = new AtomicInteger(0)
        for (range <- start to end) {
          if (stratifiedByYears.contains(range)) {
            sum.addAndGet(stratifiedByYears.get(range).get)
          }
        }
        if (tempMap.contains(key)) {
          tempMap.get(key).get.put(yearBin, sum.get())
        }
        else {
          val map = new mutable.HashMap[String, Int]()
          map.put(yearBin, sum.get())
          tempMap.put(key, map)
        }
      })
    })

    val authorBucket: Array[String] = MapReduceConfig.configObj.config.getString("AuthorCountBin").split(",")
    authorBucket.foreach(bucket => {
      if (bucket.contains("end")) {
        val bucketRange = bucket.split("-")
        yearBucket.foreach(yearBin => {
          val start = new AtomicInteger(Integer.parseInt(bucketRange(0)))
          val bin = new mutable.StringBuilder(start + "-")
          val sum = new AtomicInteger(0)
          while (start.get <= highestBin.get) {
            if (tempMap.contains(start.get)) {
              sum.addAndGet(tempMap.get(start.get).get.get(yearBin).get)
            }
            start.incrementAndGet()
          }
          val end = start.decrementAndGet()
          bin.append(end.toString)
          finalBinKey.addOne(bin.toString())
          finalBinYearKey.addOne(yearBin)
          finalBinValue.addOne(sum.get.toString)
        })
      } else if (bucket.contains("-")) {
        val bucketRange = bucket.split("-")
        yearBucket.foreach(yearBin => {
          val start = Integer.parseInt(bucketRange(0))
          val end = Integer.parseInt(bucketRange(1))
          val sum = new AtomicInteger(0)
          for (range <- start to end) {
            if (tempMap.contains(range)) {
              sum.addAndGet(tempMap.get(range).get.get(yearBin).get)
            }
          }
          finalBinKey.addOne(bucket)
          finalBinYearKey.addOne(yearBin)
          finalBinValue.addOne(sum.get.toString)
        })
      } else {
        val bin = Integer.parseInt(bucket)
        yearBucket.foreach(yearBin => {
          if (tempMap.contains(bin)) {
            finalBinKey.addOne(bin.toString)
            finalBinYearKey.addOne(yearBin)
            finalBinValue.addOne(tempMap.get(bin).get.get(yearBin).get.toString)
          } else {
            finalBinKey.addOne(bin.toString)
            finalBinYearKey.addOne(yearBin)
            finalBinValue.addOne("0")
          }
        })
      }
    })

    Array(finalBinKey, finalBinYearKey, finalBinValue)

  }

  /**
   * Utility function to run Map reduce job
   *
   * @param input
   * @param output
   * @throws
   */
  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "MapReduceJob3"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(PublicationYearStratification.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[PublicationYearStratification.Map])
    job.setReducerClass(classOf[PublicationYearStratification.Reduce])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    val outputDir = output.replace("(jobName)", jobName)
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Co-Author Bin", "Publication Year Range", "Number Of Co-Authors")
    if (job.waitForCompletion(true)) {
      ReadWriteUtils.writeToHdfs(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(outputDir)))
    } else System.exit(1)
  }
}