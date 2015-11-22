name := "enron-spark"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.5.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.5.1"
libraryDependencies += "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.4.1"
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.1.0"
