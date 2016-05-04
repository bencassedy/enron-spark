package com.bencassedy.spartakos.enron

import com.bencassedy.spartakos.common.SpartakosConfig
import com.typesafe.config.{ConfigFactory, Config}

/**
  * Project configuration settings
  */
class EnronConfig(appName: String) extends SpartakosConfig {
//  val config: Config = ConfigFactory.load(appName)

  // TODO: factor these out to a config file
  val numClusters: Int = 20
  val sampleSize: Double = 0.1
  val numTextFeatures: Int = 1000
  val idfMinDocFreq: Int = 4
  val trainingTestSplit: Array[Double] = Array(0.8, 0.2)
  val trainingIterations: Int = 100
  val inputFile: String = "enron.json"
  val outputLocation: String = "./results.txt"
}

object EnronConfig {
  def apply(appName: String) = new EnronConfig(appName)
}