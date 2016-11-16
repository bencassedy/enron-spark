package com.bencassedy.spartakos.models.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext
import com.bencassedy.spartakos.reddit.RedditConfig
import org.scalatest.FunSuite

/**
  * Class for testing functionality of the RedditSpark class;
  * also serves as quasi-documentation of schema, stats, etc.
  */
class TestRedditSpark extends FunSuite {
  // common contexts and the reddit corpus to be shared across test cases
  val settings = RedditConfig("reddit.conf")
  val (sparkContext, sqlContext) = SpartakosSparkContext.init(Option(settings))
  import sqlContext.implicits._
  val redditCorpus = sqlContext.read.parquet(settings.redditData).repartition(4)

  test("the reddit schema is what we're expecting") {
    val expectedSchema =
      "struct<archived:boolean," +
        "author:string," +
        "author_flair_css_class:string," +
        "author_flair_text:string," +
        "body:string," +
        "controversiality:bigint," +
        "created_utc:string," +
        "distinguished:string," +
        "downs:bigint," +
        "edited:string," +
        "gilded:bigint," +
        "id:string," +
        "link_id:string," +
        "name:string," +
        "parent_id:string," +
        "retrieved_on:bigint," +
        "score:bigint," +
        "score_hidden:boolean," +
        "subreddit:string," +
        "subreddit_id:string," +
        "ups:bigint>"
    assert(redditCorpus.schema.simpleString == expectedSchema)
  }

  test("the reddit corpus is ~ 102MM records") {
    assert(redditCorpus.count() == 102194289)
  }
}
