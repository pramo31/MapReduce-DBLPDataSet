package com.cloud.mapreduce

import com.cloud.mapreduce.config.MapReduceConfig
import com.cloud.mapreduce.core._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.CollectionConverters._

object MapReduce {

  final val logger: Logger = LoggerFactory.getLogger(MapReduce.getClass)


  def main(args: Array[String]): Unit = {

    logger.info("Starting Map Reduce Job")
    val configObj = MapReduceConfig.configObj
    val jobs = configObj.config.getString("JobsToRun").split(",").toList
    jobs.par.foreach(job => {
      if (job.trim.equals("AuthorCount")) {
        logger.info("Starting Co-Author Count Job")
        AuthorCount.runJob(configObj.inputFile, configObj.outputFile)
      } else if (job.trim.equals("PublicationYearStratification")) {
        logger.info("Starting Co-Author count, Stratification by Publication Year job")
        PublicationYearStratification.runJob(configObj.inputFile, configObj.outputFile)
      } else if (job.trim.equals("PublicationVenueStratification")) {
        logger.info("Starting Co-Author count, Stratification by Publication Venue job")
        PublicationVenueStratification.runJob(configObj.inputFile, configObj.outputFile)
      } else if (job.trim.equals("Authorship")) {
        logger.info("Starting Authorship and Top 100 and least 100 collaborating authors job")
        Authorship.runJob(configObj.inputFile, configObj.outputFile)
      }
      else if (job.trim.equals("MeanMedianStatistics")) {
        logger.info("Starting MeanMedianStatistics calculation job")
        MeanMedianStatistics.runJob(configObj.inputFile, configObj.outputFile)
      }
      else if (job.trim.equals("MeanMedianStatisticsWithStratification")) {
        logger.info("Starting MeanMedianStatisticsWithStratification calculation job")
        MeanMedianStatisticsWithStratification.runJob(configObj.inputFile, configObj.outputFile)
      }
    })

    logger.info("Completed All Jobs Successfully")
  }
}