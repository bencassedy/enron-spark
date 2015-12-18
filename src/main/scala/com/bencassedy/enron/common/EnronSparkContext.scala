package com.bencassedy.enron.common

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Reusable wrapper that contains instantiated spark context
  */
object EnronSparkContext {

  def init: (SparkContext, SQLContext) = {
    val conf = new SparkConf()
      .setAppName("Enron")
      .setMaster("local[*]")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    (sparkContext, sqlContext)
  }
}

