package com.bencassedy.spartakos.enron

import java.io.{File, PrintWriter}

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

object EnronFromMongo extends App {
  override def main(args: Array[String]) {

    // configure and init spark
    val conf = new SparkConf()
      .setAppName("Enron")
      .setMaster("local")
      .set("spark.driver.maxResultSize", "2g")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    // retrieve the enron docs and convert them to a dataframe
    val writer = new PrintWriter(new File("enron.json"))
    val enronDocs = getMongoDocs(sparkContext)
    enronDocs
      .map(t => t._2.toString.concat("\n"))
      .collect()
      .foreach(ln => writer.write(ln))
    writer.close()
    val enronDF = sqlContext.createDataFrame(enronDocs).cache()

    enronDF.describe()
    enronDF.show(100)
  }

  def getMongoDocs(sparkContext: SparkContext): RDD[(Object, BSONObject)] = {
    // Set up the configuration for reading from MongoDB.
    val mongoConfig = new Configuration()
    // MongoInputFormat allows us to read from a live MongoDB instance.
    // We could also use BSONFileInputFormat to read BSON snapshots.
    // MongoDB connection string naming a collection to read.
    // If using BSON, use "mapred.input.dir" to configure the directory
    // where the BSON files are located instead.
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/enron.enron_collection")

    // Create an RDD backed by the MongoDB collection.
    val documents = sparkContext.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type

    documents
  }
}