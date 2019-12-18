package com.cloud.mapreduce.config

import com.cloud.mapreduce.utils.ConfigReader
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class MapReduceConfig(val configFileName: String = Constants.configFile.toString()) {

  final val logger: Logger = LoggerFactory.getLogger(MapReduceConfig.getClass)

  val config: Config = ConfigReader.readConfig(configFileName)

  val venueMap = new mutable.HashMap[String, String]()
  createVenueMap

  private val _inputFile: String = config.getString("InputFile")
  private val _outputFile: String = config.getString("OutputFile")

  def inputFile = _inputFile

  def outputFile = _outputFile


  private def createVenueMap(): Unit = {
    logger.info("Creating Publication Venue Map")
    val venueMapConfig = config.getString("VenueMap")
    val splitVenueMap = venueMapConfig.split(";")
    for (eachMap <- splitVenueMap) {
      val keyValue = eachMap.split(":")
      venueMap.put(keyValue(0), keyValue(1))
    }
  }
}

object MapReduceConfig {
  val configObj = new MapReduceConfig()
}
