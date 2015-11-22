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

/**
  * do spark stuff on Enron
  */
object EnronSpark {
  def main(args: Array[String]) {
    // configure and init spark
    val (sparkContext, sqlContext) = EnronSparkContext.init

    val enronDF = sqlContext.read
      .json("enron.json")
      .sample(withReplacement = false, 0.08, Math.random().toLong)
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
    val hashingTF = new HashingTF().setInputCol("filteredWords").setOutputCol("rawFeatures").setNumFeatures(1000)
    val featurizedData = hashingTF.transform(filteredWordsWithCounts)

    var WSSE_Results = new ListBuffer[(Int, Double)]()
    val Array(training, test) = featurizedData.randomSplit(Array(0.8, 0.2), seed = Math.random().toLong)
    val numClusters = 20

//    1 to 30 foreach {
//      i => WSSE_Results += evaluateWSSSE(i, true)
//    }

    def evaluateWSSSE(numClusters: Int, calculateScore: Any, groupBy: Boolean = false)
    :(Int, Double, RDD[(Int, String)]) = {
      // split the featurized data into training and test sets
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(4)
      val idfModel = idf.fit(training)

      def rescaleData(df: DataFrame) :RDD[(String, Vector)] = {
        idfModel.transform(df)
          .select("$oid", "features")
          .map {
            r => (r.get(0).asInstanceOf[String], r.get(1).asInstanceOf[Vector])
          }
      }

      // convert the data into tuples of (1) the message id for re-association and (2) a sparse vector, which
      // is a tuple of (features/columns, column-index mappings, and rescaled values)
      val rescaledTrainingData: RDD[(String, Vector)] = rescaleData(training)
      val rescaledTestData: RDD[(String, Vector)] = rescaleData(test)

      // cluster the data using k-means
      val numIterations = 100
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

    WSSE_Results.foreach {
      case (n, r) => println(s"numClusters: $n with WSSSE of: $r")
    }

    val results = evaluateWSSSE(numClusters, "predict")
    val rawResultsDF = sqlContext.createDataFrame(results._3)
//    val resultsWithCounts: RDD[(Int, Seq[String], Int)] = resultsDF.rdd.map {
//      case Row(category: Int, ids: Seq[String]) => (category, ids, ids.size)
//    }
//    val newResultsDF = sqlContext.createDataFrame(resultsWithCounts)

    // append the new categories back onto the original test data set
    val remappedResultsDF = test
      .join(rawResultsDF, test("$oid") === rawResultsDF("_2"))
      .withColumnRenamed("_1", "category").drop("_2")

//    remappedResultsDF.select("category", "wordCounts").show(100, truncate = false)
    val rmr = remappedResultsDF.rollup("category", "X-Origin").count().filter("count > 5")
    rmr.sort(rmr("category"), rmr("count").desc).show(500)
    println(results._2)

    FileUtils.deleteDirectory(new File("./results.txt"))
    results._3.saveAsTextFile("./results.txt")
  }
}
