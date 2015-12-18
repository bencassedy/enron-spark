package com.bencassedy.enron

import java.io.File

import com.bencassedy.enron.common.EnronSparkContext
import com.bencassedy.enron.config._
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.bencassedy.enron.utils.Transforms._

/**
  * do spark stuff on Enron
  */
object EnronSpark extends App {
  implicit val config = new Config()

  // configure and init spark
  val (sparkContext, sqlContext) = EnronSparkContext.init

  val enronDF = sqlContext.read
    .json(config.inputFile)
    .sample(withReplacement = false, config.sampleSize, Math.random().toLong)
    .cache()

  // TODO: filter out (via regex) the giant, multiline 'forwarding' strings in the bodies; these are
  // distorting the categorization; for now, we will filter out any messages containing that string, although
  // ideally those ought to be loaded in, as they likely contain at least some substantive material
  val enronBodies = enronDF.select("_id.$oid", "X-Origin", "body").filter("body is not null").filter(!enronDF("body").contains("----- Forwarded"))

  val featurizedData = transformBodies(enronBodies, "body")
  val Array(training, test) = featurizedData.randomSplit(config.trainingTestSplit, seed = Math.random().toLong)
  val numClusters = config.numClusters

  def evaluateWSSSE(numClusters: Int, calculateScore: Any, groupBy: Boolean = false)
  :(Int, Double, RDD[(Int, String)]) = {
    // split the featurized data into training and test sets
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(config.idfMinDocFreq)
    implicit val idfModel = idf.fit(training)

    // convert the data into tuples of (1) the message id for re-association and (2) a sparse vector, which
    // is a tuple of (features/columns, column-index mappings, and rescaled values)
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

  val (_, wssseScore, results) = evaluateWSSSE(numClusters, "predict")
  val rawResultsDF = sqlContext.createDataFrame(results)
  //    val resultsWithCounts: RDD[(Int, Seq[String], Int)] = rawResultsDF.rdd.map {
  //      case Row(category: Int, ids: Seq[String]) => (category, ids, ids.size)
  //    }
  //    val newResultsDF = sqlContext.createDataFrame(resultsWithCounts)

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
