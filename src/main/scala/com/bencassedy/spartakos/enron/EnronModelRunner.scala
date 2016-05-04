package com.bencassedy.spartakos.enron

import com.bencassedy.spartakos.common.SpartakosSparkContext
import com.bencassedy.spartakos.models.Word2VecUtils._
import com.bencassedy.spartakos.utils.Transforms._

/**
  * Script to run algorithms, etc.
  */
object EnronModelRunner {
  def main(args: Array[String]) {
    implicit val config = EnronConfig("EnronModelRunner")
    val (_, sqlContext) = SpartakosSparkContext.init()
    import sqlContext.implicits._

    // drop any null/empty values in the field we care about
    val enronDF = sqlContext
      .read
      .parquet(config.inputFile)
      .select("Subject")
      .cache()
      .na
      .drop(Seq("Subject"))
      .filter($"Subject".notEqual(""))

    val tokenizedSubjects = tokenize(enronDF, "Subject", "SubjectTokens")
    val synonyms = buildWord2Vec(tokenizedSubjects, "SubjectTokens", "raptor", 100)

    synonyms.show(100)
  }
}
