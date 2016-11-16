package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext
import com.bencassedy.spartakos.utils.Transforms._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import com.bencassedy.spartakos.common.SpartakosUDF._

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
  val (sparkContext, sqlContext) = SpartakosSparkContext.init(Option(settings))
  import sqlContext.implicits._

  val redditCorpus = sqlContext.read.parquet(settings.redditData).repartition(4)
  val redditCorpusSentiments = redditCorpus.sample(withReplacement = true, 0.001, 1234).withColumn("bodySentiment", getSentiment($"body"))
//  redditCorpusSentiments.show()
  redditCorpusSentiments.groupBy("bodySentiment", "subreddit").count().show(100)
//  rescaleRedditCorpus().show(100)
//  highestScoredSubreddits().show(1000)
//  topScorer().show(100)

  def rescaleRedditCorpus(): DataFrame = {
    val Array(training, test) = redditCorpus.randomSplit(Array(0.7, 0.3), 1234)
    //  redditCorpus.groupBy("subreddit").avg("score").sort($"avg(score)".desc).show(100)
    //  redditCorpus.filter("author not like '[deleted]'").groupBy("author", "subreddit", "score").avg("score").sort($"avg(score)".desc).show(100)
    val tokenizedTraining = tokenize(training, "body", "tokens")
    val tokenizedTest = tokenize(test, "body", "tokens")

    val (rescaledCorpus, idfModel) = tfIdf(tokenizedTraining, "tokens", 100000, 4)
    rescaledCorpus
  }

  def highestScoredSubreddits(): DataFrame = {
    // wanted to determine most controversial, etc. but those all appear to be zeroed out
    redditCorpus.groupBy($"subreddit").avg("score").sort($"avg(score)".desc)
  }

  def stats(): Double = {
    redditCorpus.stat.cov("score", "foo")
  }

  def topScorer(): DataFrame = {
    redditCorpus.groupBy("author").avg("score").sort($"avg(score)".desc)
  }
}
