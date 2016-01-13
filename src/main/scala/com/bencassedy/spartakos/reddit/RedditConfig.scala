package com.bencassedy.spartakos.reddit

import com.bencassedy.spartakos.common.SpartakosConfig
import com.typesafe.config.{ConfigFactory, Config}

class RedditConfig(appName: String) extends SpartakosConfig {
  val config: Config = ConfigFactory.load(appName)

  val redditData = config.getString("reddit.data.dir")
  val sparkExecutorCores = config.getString("spark.executor.cores")
}

object RedditConfig {
  def apply(appName: String) = new RedditConfig(appName)
}