package com.bencassedy.spartakos.reddit

import com.typesafe.config.{ConfigFactory, Config}

class RedditConfig(appName: String) {
  val config: Config = ConfigFactory.load(appName)

  val redditData = config.getString("reddit.data.dir")
}

object RedditConfig {
  def apply(appName: String) = new RedditConfig(appName)
}