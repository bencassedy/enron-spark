import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import common.EnronSparkContext
import utils.EnronUtils._
import config._

/**
  * do spark stuff on Enron
  */
object EnronSpark {
  def main(args: Array[String]) {
    // configure and init spark
    val (sparkContext, sqlContext) = EnronSparkContext.init
    val config = new Config()

    val enronDF = sqlContext.read
      .json(config.inputFile)
      .sample(withReplacement = false, config.sampleSize, Math.random().toLong)
      .cache()
    val enronBodies = enronDF.select("_id.$oid", "X-Origin", "body").filter("body is not null")

    // convert bodies into tf-idf vectors by (1) tokenizing the text, (2) removing stopwords, (3) adding in word count
    // mappings (via a UDF) that we will use later on for analysis, (4) hashing the term frequency values, and
    // (5) rescaling the TF data based on IDF
    val tokenizer = new RegexTokenizer().setInputCol("body").setOutputCol("words").setPattern("\\w+").setGaps(false)
    val wordsData = tokenizer.transform(enronBodies)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords")
    val filteredWords = remover.transform(wordsData)
    val filteredWordsWithCounts = filteredWords.withColumn("wordCounts", wordCounts(filteredWords("filteredWords")))
    val hashingTF = new HashingTF().setInputCol("filteredWords").setOutputCol("rawFeatures").setNumFeatures(config.numTextFeatures)
    val featurizedData = hashingTF.transform(filteredWordsWithCounts)

    var WSSSE_Results = sparkContext.accumulator(new ListBuffer[(Int, Double)]())
    val Array(training, test) = featurizedData.randomSplit(config.trainingTestSplit, seed = Math.random().toLong)
    val numClusters = config.numClusters

//    1 to 30 foreach {
//      i => WSSSE_Results += evaluateWSSSE(i, true)
//    }

    def evaluateWSSSE(numClusters: Int, calculateScore: Any, groupBy: Boolean = false)
    :(Int, Double, RDD[(Int, String)]) = {
      // split the featurized data into training and test sets
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(config.idfMinDocFreq)
      val idfModel = idf.fit(training)

      def rescaleData(df: DataFrame) :RDD[(String, Vector)] = {
        idfModel.transform(df)
          .select("$oid", "features")
          .map {
            row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Vector])
          }
      }

      // convert the data into tuples of (1) the message id for re-association and (2) a sparse vector, which
      // is a tuple of (features/columns, column-index mappings, and rescaled values)
      val rescaledTrainingData: RDD[(String, Vector)] = rescaleData(training)
      val rescaledTestData: RDD[(String, Vector)] = rescaleData(test)

      // cluster the data using k-means
      val numIterations = config.trainingIterations
      val clusters: KMeansModel = KMeans.train(rescaledTrainingData.map(_._2), numClusters, numIterations)
      val WSSSE: Double = clusters.computeCost(rescaledTrainingData.map(_._2))

//      val groupByResults: RDD[(Int, Seq[String])] = rescaledTestData.map {
//        case (id :String, vector) =>
//          (clusters.predict(vector), id)
//      }.groupByKey().mapValues(_.asInstanceOf[Seq[String]])
      val categorizedResults = rescaledTestData.map {
        case (id :String, vector) =>
          (clusters.predict(vector), id)
      }

      (numClusters, WSSSE, categorizedResults)
    }

    WSSSE_Results.localValue.foreach {
      case (n, r) => println(s"numClusters: $n with WSSSE of: $r")
    }

    val (_, wssseScore, results) = evaluateWSSSE(numClusters, "predict")
//    val rawResultsDF = sqlContext.createDataFrame(results._3)
//    val resultsWithCounts: RDD[(Int, Seq[String], Int)] = resultsDF.rdd.map {
//      case Row(category: Int, ids: Seq[String]) => (category, ids, ids.size)
//    }
//    val newResultsDF = sqlContext.createDataFrame(resultsWithCounts)

    // append the new categories back onto the original test data set
//    val remappedResultsDF = test
//      .join(rawResultsDF, test("$oid") === rawResultsDF("_2"))
//      .withColumnRenamed("_1", "category").drop("_2")

//    remappedResultsDF.select("category", "wordCounts").show(100, truncate = false)
//    val rmr = remappedResultsDF.rollup("category", "X-Origin").count().filter("count > 5")
//    rmr.sort(rmr("category"), rmr("count").desc).show(500)
//    println(results._2)

    ConfigParser.parser.parse(args, Config()) match {
      case Some(cfg) =>
        // if we are using a non-s3 output location, delete the file before writing
        if (args.length == 0 || !cfg.outputLocation.startsWith("s3://"))
          FileUtils.deleteDirectory(new File(config.outputLocation))
        results.saveAsTextFile(config.outputLocation)

      case None =>
        // arguments are bad, error message will have been displayed
    }
  }
}
