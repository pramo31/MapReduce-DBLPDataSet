package com.cloud.mapreduce.core

import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

object SortingJob {

  val logger: Logger = LoggerFactory.getLogger(SortingJob.getClass)

  /*
    class AuthorshipComparator extends WritableComparator(classOf[DoubleWritable]) {
      override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
        val v1 = ByteBuffer.wrap(b1, s1, l1).getDouble
        val v2 = ByteBuffer.wrap(b2, s2, l2).getDouble
        v1.compareTo(v2) * (-1)
      }
    }
   */


  class Map extends Mapper[LongWritable, Text, DoubleWritable, Text] {

    /**
     * Sorting by value map job which reverses the input key and value as output value and key respectively
     *
     * @param key
     * @param value
     * @param context
     * @throws
     * @throws
     */
    @throws[java.io.IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, DoubleWritable, Text]#Context): Unit = {
      val line = value.toString
      val tokens = line.split(",") // This is the delimiter between Key and Value
      context.write(new DoubleWritable(tokens(1).toDouble), new Text(tokens(0)))
    }
  }

  class Reduce extends Reducer[DoubleWritable, Text, Text, DoubleWritable] {

    /**
     * Sorting by value reduce job which reverses the input key and value as output value and key respectively and gets the original key value pairs but in sorted order
     *
     * @param key
     * @param list
     * @param context
     * @throws
     * @throws
     */
    @throws[java.io.IOException]
    @throws[InterruptedException]
    override def reduce(key: DoubleWritable, list: Iterable[Text], context: Reducer[DoubleWritable, Text, Text, DoubleWritable]#Context): Unit = {
      list.forEach(value => {
        context.write(value, key)
      })
    }
  }

  @throws[Exception]
  def runJob(input: String, output: String, inputJobName: String): Unit = {
    val conf = new Configuration
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    val jobName = "Sorted" + inputJobName
    val job = Job.getInstance(conf, jobName)
    job.setJarByClass(SortingJob.getClass)
    job.setOutputKeyClass(classOf[DoubleWritable])
    job.setOutputValueClass(classOf[Text])
    job.setMapOutputKeyClass(classOf[DoubleWritable])
    job.setMapOutputValueClass(classOf[Text])
    job.setMapperClass(classOf[SortingJob.Map])
    job.setReducerClass(classOf[SortingJob.Reduce])
    //job.setSortComparatorClass(classOf[SortingJob.AuthorshipComparator])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output.replace("(jobName)", jobName)))
    if (job.waitForCompletion(true)) return else System.exit(1)
  }
}
