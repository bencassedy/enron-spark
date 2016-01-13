package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext
import com.bencassedy.spartakos.reddit.RedditConfig$
import org.apache.log4j.Logger

/**
  * do Spark stuff on Reddit comment corpus
  */
object RedditSpark extends App {
  val logger = Logger.getLogger(RedditSpark.getClass)
  val settings = RedditConfig("reddit.conf")
  val (sparkContext, sqlContext) = SpartakosSparkContext.init


}
