package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext
import org.apache.log4j.Logger

import com.bencassedy.spartakos.common.SpartakosUDF._
import com.bencassedy.spartakos.reddit.RedditDataFrame._

/**
  * do Spark stuff on Reddit comment corpus
  *
  * Some questions to answer: correlation between ups and controversiality
  * What is the most controversial subreddit?
  * Who is the most upvoted author and in what subreddits?
  * What does sentiment look like compared to score?
  *
  */
object RedditSpark extends App {
  val logger = Logger.getLogger(RedditSpark.getClass)
  val settings = RedditConfig("reddit.conf")
  implicit val (sparkContext, sqlContext) = SpartakosSparkContext.init(Option(settings))
  import sqlContext.implicits._

  val redditCorpus = sqlContext.read.parquet(settings.redditData).repartition(4)
  val redditCorpusSentiments = redditCorpus.generateSentiments()
  val rescaled = redditCorpus.rescale()

  redditCorpusSentiments.groupBy("bodySentiment", "subreddit").count().show(100)
  redditCorpus.topScorer().show(100)
}
