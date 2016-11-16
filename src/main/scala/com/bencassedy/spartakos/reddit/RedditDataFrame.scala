package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosUDF._
import com.bencassedy.spartakos.utils.Transforms._
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Implicit conversion pattern for reddit dataframe
  */
object RedditDataFrame {
  implicit class RedditDF(val underlying: DataFrame) {
    def rescale(): DataFrame = {
      val Array(training, test) = underlying.randomSplit(Array(0.7, 0.3), 1234)
      //  redditCorpus.groupBy("subreddit").avg("score").sort($"avg(score)".desc).show(100)
      //  redditCorpus.filter("author not like '[deleted]'").groupBy("author", "subreddit", "score").avg("score").sort($"avg(score)".desc).show(100)
      val tokenizedTraining = tokenize(training, "body", "tokens")
      val tokenizedTest = tokenize(test, "body", "tokens")

      val (rescaledCorpus, idfModel) = tfIdf(tokenizedTraining, "tokens", 100000, 4)
      rescaledCorpus
    }

    def highestScoredSubreddits()(implicit sqlContext: SQLContext): DataFrame = {
      // wanted to determine most controversial, etc. but those all appear to be zeroed out
      import sqlContext.implicits._
      underlying.groupBy($"subreddit").avg("score").sort($"avg(score)".desc)
    }

    def stats(): Double = {
      underlying.stat.cov("score", "foo")
    }

    def topScorer()(implicit sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._
      underlying.groupBy("author").avg("score").sort($"avg(score)".desc)
    }

    def generateSentiments()(implicit sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._
      underlying
        .sample(withReplacement = true, 0.001, 1234)
        .withColumn("bodySentiment", getSentiment($"body"))
    }
  }
}
