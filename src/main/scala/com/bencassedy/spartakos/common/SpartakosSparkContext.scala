package com.bencassedy.spartakos.common

import com.bencassedy.spartakos.reddit.RedditConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Reusable wrapper that contains instantiated spark context
  */
object SpartakosSparkContext {
  def init(spc: Option[SpartakosConfig] = None): (SparkContext, SQLContext) = {
    val sparkConf = new SparkConf()
      .setAppName("Enron")
      .setMaster("local[*]")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
      .set("spark.worker.cores", "1")
      .set("spark.executor.heartbeatInterval", "30")
    spc match {
      case spc: Some[RedditConfig] => sparkConf.set("spark.executor.cores", spc.get.sparkExecutorCores)
      case _ =>
    }

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    (sparkContext, sqlContext)
  }
}

