package com.bencassedy.spartakos.enron

import java.io.File

import com.bencassedy.spartakos.common.SpartakosSparkContext
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import com.bencassedy.spartakos.utils.Transforms._

/**
  * do spark stuff on Enron
  */
object EnronSpark extends App {
  implicit val config = new Config()

  // configure and init spark
  val (sparkContext, sqlContext) = SpartakosSparkContext.init

  val enronDF = sqlContext.read.json(config.inputFile)
    .sample(withReplacement = false, config.sampleSize, Math.random().toLong).cache()

  // TODO: filter out (via regex) the giant, multiline 'forwarding' strings in the bodies; these are
  // distorting the categorization; for now, we will filter out any messages containing that string, although
  // ideally those ought to be loaded in, as they likely contain at least some substantive material
  val enronBodies = enronDF.select("_id.$oid", "X-Origin", "body").filter("body is not null").filter(!enronDF("body").contains("----- Forwarded"))

  val featurizedData = transformBodies(enronBodies, "body")
  val Array(training, test) = featurizedData.randomSplit(config.trainingTestSplit, seed = Math.random().toLong)
  val numClusters = config.numClusters

  /**
    * Create the predefined number of clusters using k-means. This function will also return the WSSSE statistic to
    * assess the standard error for the clusters. Based on previous model runs with this data set, the inflection point,
    * i.e., the sweet spot for number of clusters, comes in at about 20 clusters.
    *
    * @param numClusters the number of clusters to create
    * @return an RDD of (Int, String) tuples, where _1 is the category, and _2 is the doc id, which we'll use
    *         later to map the category back onto the full record
    */
  def createClusters(numClusters: Int): (Int, Double, RDD[(Int, String)]) = {
    // split the featurized data into training and test sets
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(config.idfMinDocFreq)
    implicit val idfModel = idf.fit(training)

    // convert the data into tuples of (1) the message id for re-association and (2) a sparse vector,
    // which is a tuple of (features/columns, column-index mappings, and rescaled values)
    val rescaledTrainingData: RDD[(String, Vector)] = rescaleData(training)
    val rescaledTestData: RDD[(String, Vector)] = rescaleData(test)

    // cluster the data using k-means
    val numIterations = config.trainingIterations
    val clusters: KMeansModel = KMeans.train(rescaledTrainingData.map(_._2), numClusters, numIterations)
    val WSSSE: Double = clusters.computeCost(rescaledTrainingData.map(_._2))

    val categorizedResults = rescaledTestData.map {
      case (id :String, vector) =>
        (clusters.predict(vector), id)
    }

    (numClusters, WSSSE, categorizedResults)
  }

  val (_, wssseScore, results) = createClusters(numClusters)
  val rawResultsDF = sqlContext.createDataFrame(results)

  // append the new categories back onto the original test data set
  val remappedResultsDF = test
    .join(rawResultsDF, test("$oid") === rawResultsDF("_2"))
    .withColumnRenamed("_1", "category").drop("_2")

  ConfigParser.parser.parse(args, Config()) match {
    case Some(cfg) =>
      // if we are using a non-s3 output location, delete the file before writing
      if (args.length == 0 || !cfg.outputLocation.startsWith("s3://"))
        FileUtils.deleteDirectory(new File(config.outputLocation))
      remappedResultsDF.write.save(config.outputLocation)
    case None =>
    // arguments are bad, error message will have been displayed
  }
}
