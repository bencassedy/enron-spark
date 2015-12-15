library(SparkR)

sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)

df <- createDataFrame(sqlContext, iris)
head(df)

model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")

# Java ref type org.apache.spark.ml.PipelineModel
print(model)
f = file ("/Users/bencassedy/IdeaProjects/enron-spark/src/main/resources/glm.model", "wb")

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(value)
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes=TRUE)
}

writeString(f, model@model@id)
close(f)