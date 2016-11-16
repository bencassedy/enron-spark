package com.bencassedy.spartakos.models

import org.scalatest.FunSuite

/**
  * Class for testing sentiment of text bodies
  */
class TestSentimentAnalysis extends FunSuite {

  test("negative sentence apparently indicates very positive sentiment") {
    val negativeSentence = "Evil will always triumph because good is dumb."
    val annotatedDoc = SentimentAnalysis.detectSentiment(negativeSentence)

    assert(annotatedDoc === "Very positive")
  }

  test("neutral sentence should indicate neutral sentiment") {
    val neutralSentence = "there can be only one"
    val annotatedDoc = SentimentAnalysis.detectSentiment(neutralSentence)

    assert(annotatedDoc === "Neutral")
  }

  test("trying another negative sentence") {
    val theCaptain = "What we've got here is failure to communicate."
    val annotatedDoc = SentimentAnalysis.detectSentiment(theCaptain)

    assert(annotatedDoc === "Negative")
  }

  test("mega positive sentence apparently indicates only moderately positive sentiment") {
    val stuartSmalley = "I'm good enough, I'm smart enough, and doggone it, people like me."
    val annotatedDoc = SentimentAnalysis.detectSentiment(stuartSmalley)

    assert(annotatedDoc === "Positive")
  }

  test("the most negative song lyrics aren't THAT negative of a sentiment") {
    val larsUllrich = "Darkness Imprisoning me. All that I see. Absolute horror. I cannot live. I cannot die"
    val annotatedDoc = SentimentAnalysis.detectSentiment(larsUllrich)

    assert(annotatedDoc === "Negative")
  }
}
