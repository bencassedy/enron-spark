package config

/**
  * Project configuration settings
  */
case class Config(
                   numClusters: Int = 20,
                   sampleSize: Double = 0.0001,
                   numTextFeatures: Int = 1000,
                   idfMinDocFreq: Int = 4,
                   trainingTestSplit: Array[Double] = Array(0.8, 0.2),
                   trainingIterations: Int = 100,
                   inputFile: String = "enron.json",
                   outputLocation: String = "./results.txt"
                 )



object ConfigParser {
  val parser = new scopt.OptionParser[Config]("enron-spark") {
    head ("enron-spark", "1.0")
    opt[String] ("output") action {
      (param, config) =>
        config.copy (outputLocation = param)
    } text "Optional file output location. Defaults to './results.txt'"
  }
}
