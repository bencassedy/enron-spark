import com.typesafe.config.{ConfigFactory, Config}

class Config(appName: String) {
  val config: Config = ConfigFactory.load(appName)

  val kafkaBrokers = config.getString("kafka.brokers")
  val kafkaTopic = config.getString("kafka.topic")
  val kafkaOffsetReset = config.getString("kafka.auto.offset.reset")
  // the 'fromOffset' parameter is either going to be a number or undefined, hence this logic
  val kafkaFromOffset = if (config.hasPath("kafka.from.offset")) config.getInt("kafka.from.offset") else None

  val sparkAppName = config.getString("spark.app.name")
  val sparkUIPort = config.getString("spark.ui.port")
  val sparkLocal = config.getBoolean("spark.local")
  val sparkDriverResultSize = config.getString("spark.driver.maxResultSize")
  val sparkDriverMemory = config.getString("spark.driver.memory")
  val sparkExecutorMemory = config.getString("spark.executor.memory")
  val sparkStreamingIntervalSeconds = config.getInt("spark.streaming.interval.seconds")
  val sparkStreamingTimeoutMs = config.getInt("spark.streaming.timeout.ms")

  val localMessageDir = config.getString("local.message.directory")
  val localOutputDirName = config.getString("local.output.directory.name")

  val s3RScriptPath = config.getString("s3.r.script")
  val s3BucketName = config.getString("s3.bucket.name")

}

object Config {
  def apply(appName: String) = new Config(appName)
}