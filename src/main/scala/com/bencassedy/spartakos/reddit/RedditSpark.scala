package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext
import com.bencassedy.spartakos.utils.Transforms._
import org.apache.log4j.Logger

/**
  * do Spark stuff on Reddit comment corpus
  */
object RedditSpark extends App {
  val logger = Logger.getLogger(RedditSpark.getClass)
  val settings = RedditConfig("reddit.conf")
  val (sparkContext, sqlContext) = SpartakosSparkContext.init(Option(settings))
  import sqlContext.implicits._

  val redditCorpus = sqlContext.read.json(settings.redditData)
  val Array(training, test) = redditCorpus.randomSplit(Array(0.7, 0.3), 1234)
//  redditCorpus.show(100)
//  redditCorpus.groupBy("subreddit").avg("score").sort($"avg(score)".desc).show(100)
//  redditCorpus.filter("author not like '[deleted]'").groupBy("author", "subreddit", "score").avg("score").sort($"avg(score)".desc).show(100)
  val tokenizedTraining = tokenize(training, "body", "tokens")
  val tokenizedTest = tokenize(test, "body", "tokens")

  val (rescaledCorpus, idfModel) = tfIdf(tokenizedTraining, "tokens", 100000, 4)

  rescaledCorpus.show(100)
}
