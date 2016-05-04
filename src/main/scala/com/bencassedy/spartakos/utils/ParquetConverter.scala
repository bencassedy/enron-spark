package com.bencassedy.spartakos.utils

import com.bencassedy.spartakos.common.SpartakosSparkContext

/**
  * Utility script to perform a one-off conversion of text-based
  * input data to Parquet format, which seems to improve read
  * performance considerably
  */
object ParquetConverter {
  def main(args: Array[String]) {
    if (args.length < 2) println("input file argument is required"); sys.exit(1)

    val (_, sqlContext) = SpartakosSparkContext.init
    sqlContext.read.json(args(1)).write.parquet(args(1) + ".parquet")
  }
}
