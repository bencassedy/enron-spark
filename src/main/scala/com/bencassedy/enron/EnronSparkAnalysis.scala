package com.bencassedy.enron

import com.bencassedy.enron.common.EnronSparkContext
import com.bencassedy.enron.config.Config

/**
  * application for analyzing the results of the knn clustering on a set of
  * test data from Enron; we assume that the clustering application has completed and
  * stored the results in a local path
  */
object EnronSparkAnalysis extends App {
  // configure and init spark
  val (sparkContext, sqlContext) = EnronSparkContext.init
  val config = new Config()
  import sqlContext.implicits._

  // category counts:
//  +--------+-----+
//  |category|count|
//  +--------+-----+
//  |       0| 9867|
//  |       2|    5|
//  |       3|   16|
//  |       5|    4|
//  |       7|    6|
//  |       8|   20|
//  |       9|  276|
//  |      10|  222|
//  |      13|    3|
//  |      14|    1|
//  |      15|   14|
//  |      17|    1|
//  |      18|    3|
//  |      19|    1|
//  +--------+-----+

  // load the test results
  val testResults = sqlContext.read.load(config.outputLocation).cache()

  val testGroupings = testResults.select("category", "wordCounts")
    .explode("wordCounts", "wordCount") {
      wordTuples: Seq[(String, Int)] => wordTuples
    }

  testGroupings.groupBy("category", "wordCount").count().filter("count > 4").sort($"category", $"count".desc).show(1000)

  // Category 0 is almost entirely the day-to-day type of 'business' emails you see on a regular
  // basis. This is pretty interesting in that it could be used to snowball more business-related emails

  // Category 3 is mostly WEEKEND SYSTEM OUTAGES with a few false positives

  // Category 7 is forwards to large groups; looks like the category was derived entirely from
  // the presence of the forward list words, i.e., it's sort of a spurious category

  // Category 8 is mostly news article newsletters, some job/alumni newsletters; seems like a reasonable
  // categorization

  // Category 9 is more garden-variety newsletter-type stuff
  // Category 10 is more forwarding spuriousness
  // Category 13 is a few fantasy football emails with a bunch of html
  // Category 15 is a bunch of [IMAGE] tagged records
  // Category 18 is more html tag stuff

//  testResults.filter("category = 15").select("body").show(100, truncate = false)
  //      val groupByResults: RDD[(Int, Seq[String])] = rescaledTestData.map {
  //        case (id :String, vector) =>
  //          (clusters.predict(vector), id)
  //      }.groupByKey().mapValues(_.asInstanceOf[Seq[String]])


  /**
    * show the top word count words, then group by category then origin, so we can
    * get a sense of the per-custodian distribution of categories
    */
  def showOrigins() = {
    val rmr = testResults.rollup("category", "X-Origin").count().filter("count > 5")
    rmr.sort(rmr("category"), rmr("count").desc).show(500)
  }
}

case class WordCountCategory(word: String, docCount: Int, category: Int)
