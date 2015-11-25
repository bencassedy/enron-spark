name := "enron-spark"
version := "1.0"
scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.5.1" % "provided"
libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.1" % "provided"
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.1.0" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

// build an assembly jar
assemblyJarName in assembly := "enron-spark.jar"
mainClass in assembly := Some("EnronSpark")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)                 => MergeStrategy.discard
  case "results.txt"                                 => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// enable deployment of artifact to s3 bucket
import S3._

s3Settings

mappings in upload := Seq((new java.io.File("target/scala-2.11/enron-spark.jar"), "enron-spark.jar"))
host in upload := "bencassedyenron.s3.amazonaws.com"
credentials += Credentials(Path.userHome / ".aws/s3credentials")