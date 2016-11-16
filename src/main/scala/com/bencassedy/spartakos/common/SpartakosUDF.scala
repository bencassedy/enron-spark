package com.bencassedy.spartakos.common

import com.bencassedy.spartakos.models.SentimentAnalysis
import org.apache.spark.sql.functions.udf

/**
  * Singleton object for storing various dataframe user-defined functions
  */
object SpartakosUDF {
  // perform sentiment analysis on a dataframe string column
  val sentimentCoder: String => String = (arg: String) => {
    SentimentAnalysis.detectSentiment(arg)
  }
  val getSentiment = udf(sentimentCoder)

}
