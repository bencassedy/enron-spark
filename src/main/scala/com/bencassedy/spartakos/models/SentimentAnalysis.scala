package com.bencassedy.spartakos.models

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass

/**
  *
  */
object SentimentAnalysis {
  val properties = new Properties()
  properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  properties.setProperty("ssplit.isOneSentence", "true")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  def detectSentiment(message: String): String = {
    val doc: Annotation = new Annotation(message)
    pipeline.annotate(doc)
    // note that the 'get(0)' call will only work correctly if ssplit.isOneSentence is set to true
    val sentiment: String = doc.get(classOf[SentencesAnnotation]).get(0).get(classOf[SentimentClass])

    sentiment
  }
}
