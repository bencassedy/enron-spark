package com.bencassedy.spartakos.enron

import com.bencassedy.spartakos.common.SpartakosSparkContext

/**
  * Script to run algorithms, etc.
  */
object EnronModelRunner {
  def main(args: Array[String]) {
    implicit val config = EnronConfig("EnronModelRunner")

    // configure and init spark
    val (sparkContext, sqlContext) = SpartakosSparkContext.init()

    val enronDF = sqlContext.read.json(config.inputFile)
  }
}
