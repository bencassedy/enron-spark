package com.bencassedy.spartakos.models

import org.apache.spark.ml.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.sql.DataFrame

/**
  * Singleton object to run word2vec algorithm on dataframe
  */
object Word2Vec {

  def word2Vec(df: DataFrame, searchTerm: String, numSynonyms: Int): DataFrame = {
    val word2vec = new Word2Vec()
    val model = word2vec.fit(df)
    val synonyms = model.findSynonyms(searchTerm, numSynonyms)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    synonyms
  }

  def saveModel(model: Word2VecModel, modelPath: String): Word2VecModel = {
    model.save(modelPath)
    Word2VecModel.load(modelPath)
  }
}
