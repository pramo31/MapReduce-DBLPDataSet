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

object PublicationVenueStratification {

  val logger: Logger = LoggerFactory.getLogger(PublicationVenueStratification.getClass)

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {

    /**
     * Map function which maps "author count-venue' to value 1
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
        val outValue = new IntWritable(1)
        val authorVenueMap: mutable.HashMap[String, String] = MapReduceConfig.configObj.venueMap
        var firstTag = true
        var dblpElement = ""
        var venue = ""
        while (reader.hasNext) {
          try {
            reader.next
            if (reader.isStartElement) {
              if (firstTag) {
                dblpElement = reader.getLocalName
                if (authorVenueMap.exists(venueMap => venueMap._1 == dblpElement)) {
                  venue = authorVenueMap.get(dblpElement).get

                }
                firstTag = false
              }
              val currentElement = reader.getLocalName
              if (currentElement eq "author") {
                authorCount += 1
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
        if (authorCount > 0 && venue != "") {
          val output = authorCount + "-" + venue
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
     * Reduce function does a simple count of the values wich will be count of the no of keys or 'author count-venue' occurings
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
   * Utility function to get final csv values by splitting values from the map reduce output
   *
   * @param input
   * @return
   */
  def getFinalCSV(input: String): Array[ArrayBuffer[String]] = {
    val lines: String = ReadWriteUtils.readHdfsPartFiles(input)
    val line: Array[String] = lines.split("\n")
    val splitLines: Array[Array[String]] = line.filter(each => !StringUtils.isBlank(each)).map(each => each.split(","))
    val authorCount = new mutable.HashMap[Int, mutable.HashMap[String, Int]]()
    val highestBin = new AtomicInteger(0)

    splitLines.foreach(each => {
      val compositeKey = each(0).split("-")
      val bin = Integer.parseInt(compositeKey(0))
      val venue = compositeKey(1)
      val count = Integer.parseInt(each(1))
      if (authorCount.contains(bin)) {
        authorCount.get(bin).get.put(venue, count)
      }
      else {
        val stratify = new mutable.HashMap[String, Int]()
        stratify.put(venue, count)
        authorCount.put(bin, stratify)
      }

      if (bin > highestBin.get()) {
        highestBin.set(bin)
      }
    })

    val finalBinKey = new ArrayBuffer[String]
    val finalBinVenueKey = new ArrayBuffer[String]
    val finalBinValue = new ArrayBuffer[String]

    val venueBucket = MapReduceConfig.configObj.venueMap.values.toSet.toList
    val authorBucket: Array[String] = MapReduceConfig.configObj.config.getString("AuthorCountBin").split(",")
    authorBucket.foreach(bucket => {
      if (bucket.contains("end")) {
        val bucketRange = bucket.split("-")
        venueBucket.foreach(venueBin => {
          val start = new AtomicInteger(Integer.parseInt(bucketRange(0)))
          val bin = new mutable.StringBuilder(start + "-")
          val sum = new AtomicInteger(0)
          while (start.get <= highestBin.get) {
            if (authorCount.contains(start.get)) {
              val temp = authorCount.get(start.get).get
              if (temp.contains(venueBin)) {
                sum.addAndGet(temp.get(venueBin).get)
              }
            }
            start.incrementAndGet()
          }
          val end = start.decrementAndGet()
          bin.append(end.toString)
          finalBinKey.addOne(bin.toString())
          finalBinVenueKey.addOne(venueBin)
          finalBinValue.addOne(sum.get.toString)
        })
      } else if (bucket.contains("-")) {
        val bucketRange = bucket.split("-")
        venueBucket.foreach(venueBin => {
          val start = Integer.parseInt(bucketRange(0))
          val end = Integer.parseInt(bucketRange(1))
          val sum = new AtomicInteger(0)
          for (range <- start to end) {
            if (authorCount.contains(range)) {
              val temp = authorCount.get(range).get
              if (temp.contains(venueBin)) {
                sum.addAndGet(temp.get(venueBin).get)
              }
            }
          }
          finalBinKey.addOne(bucket)
          finalBinVenueKey.addOne(venueBin)
          finalBinValue.addOne(sum.get.toString)
        })
      } else {
        val bin = Integer.parseInt(bucket)
        venueBucket.foreach(venueBin => {
          if (authorCount.contains(bin)) {
            val temp = authorCount.get(bin).get
            if (temp.contains(venueBin)) {
              finalBinKey.addOne(bin.toString)
              finalBinVenueKey.addOne(venueBin)
              finalBinValue.addOne(temp.get(venueBin).get.toString)
            } else {
              finalBinKey.addOne(bin.toString)
              finalBinVenueKey.addOne(venueBin)
              finalBinValue.addOne("0")
            }
          } else {
            finalBinKey.addOne(bin.toString)
            finalBinVenueKey.addOne(venueBin)
            finalBinValue.addOne("0")
          }
        })
      }
    })
    Array(finalBinKey, finalBinVenueKey, finalBinValue)
  }

  @throws[Exception]
  def runJob(input: String, output: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "MapReduceJob2"
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(PublicationVenueStratification.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[PublicationVenueStratification.Map])
    job.setReducerClass(classOf[PublicationVenueStratification.Reduce])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    val outputDir = output.replace("(jobName)", jobName)
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(outputDir))
    val header: Array[String] = Array("Co-Author Bin", "Publication Venue", "Number Of Co-Authors")
    if (job.waitForCompletion(true)) {
      ReadWriteUtils.writeToHdfs(outputDir, jobName, ParserUtils.formatCsv(header, getFinalCSV(outputDir)))
    } else System.exit(1)
  }
}
