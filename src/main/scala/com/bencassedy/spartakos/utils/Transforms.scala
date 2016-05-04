package com.bencassedy.spartakos.utils

import com.bencassedy.spartakos.enron.EnronConfig
import com.bencassedy.spartakos.utils.StringUtils._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Functions for performing pipeline transformations on DataFrames
  */
object Transforms {

  /**
    * convert bodies into tf-idf vectors by (1) tokenizing the text, (2) removing stopwords, (3) adding in word count
    * mappings (via a UDF) that we will use later on for analysis, (4) hashing the term frequency values, and
    * (5) rescaling the TF data based on IDF
    *
    * @param df dataframe of email body payloads
    * @return
    */
  def transformBodies(df: DataFrame, colName: String)(implicit config: EnronConfig): DataFrame = {
    // the transformers
    val ngramizer = new NGram().setN(3).setInputCol("filteredWords").setOutputCol("ngrams")
    val hasher = new HashingTF().setInputCol("ngrams").setOutputCol("rawFeatures").setNumFeatures(config.numTextFeatures)

    // apply the transformers to the data
    val filteredWords = tokenize(df, colName, "filteredWords")
    val ngrams = ngramizer.transform(filteredWords)
    val filteredWordsWithCounts = ngrams.withColumn("wordCounts", wordCounts(ngrams("ngrams")))

    hasher.transform(filteredWordsWithCounts)
  }

  def tokenize(df: DataFrame, inputColName: String, outputColName: String): DataFrame = {
    // define the transformations
    val tokenizer = new RegexTokenizer().setInputCol(inputColName).setOutputCol("words").setPattern("\\w+").setGaps(false)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol(outputColName)

    // map the transformations onto the data
    val wordsData = tokenizer.transform(df)

    remover.transform(wordsData)
  }

  def tfIdf(df: DataFrame, inputColName: String, numFeatures: Int, minDocFreq: Int): (DataFrame, IDFModel) = {
    // define the transformations
    val hasher = new HashingTF().setInputCol(inputColName).setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(minDocFreq)

    // map the transformations onto the data
    val hashed = hasher.transform(df)
    val idfModel = idf.fit(hashed)

    (idfModel.transform(hashed), idfModel)
  }

  /**
    * this function accepts a dataframe with a column named 'features', which is presumably the output column
    * of one or more pipeline transformations on an original dataframe. It will then calculate inverse doc frequency,
    * and return the idf-rescaled dataframe
    *
    * @param df the dataframe to be rescaled
    * @param idfModel the IDF model that will perform the rescaling
    * @return dataframe identical to the input df, but with idf-rescaled features
    */
  def rescaleData(df: DataFrame)(implicit idfModel: IDFModel): RDD[(String, Vector)] = {
    idfModel.transform(df)
      .select("$oid", "features")
      .map {
        row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Vector])
      }
  }
}
