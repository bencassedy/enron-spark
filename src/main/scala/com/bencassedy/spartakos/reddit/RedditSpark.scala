package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosSparkContext

/**
  * do Spark stuff on Reddit comment corpus
  */
object RedditSpark extends App {
  val (sparkContext, sqlContext) = SpartakosSparkContext.init


}
