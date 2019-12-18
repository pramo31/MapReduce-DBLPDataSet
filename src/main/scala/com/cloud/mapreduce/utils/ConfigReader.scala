package com.cloud.mapreduce.utils

import java.io.IOException

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

object ConfigReader {

  val logger: Logger = LoggerFactory.getLogger(ConfigReader.getClass)

  @throws[IOException]
  def readConfig(configFileName: String): Config = {
    logger.debug(String.format("Reading configuration file %s.", configFileName))
    ConfigFactory.load(configFileName)
  }
}