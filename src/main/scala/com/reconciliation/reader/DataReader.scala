package com.reconciliation.reader

import com.reconciliation.model.ReconConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  def read(spark: SparkSession, config: ReconConfig): DataFrame = {
    config.sourceType.toLowerCase match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .csv(config.sourceLocation)
      case "excel" =>
        spark.read
          .format("com.crealytics.spark.excel")
          .option("header", "true")
          .load(config.sourceLocation)
      case "text" =>
        spark.read.textFile(config.sourceLocation).toDF()
      case "dat" =>
        spark.read
          .option("header", "true")
          .option("delimiter", "|")
          .csv(config.sourceLocation)
      case "parquet" =>
        spark.read.parquet(config.sourceLocation)
      case "orc" =>
        spark.read.orc(config.sourceLocation)
      case "hive" =>
        spark.sql(s"SELECT * FROM ${config.sourceLocation}")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported source type: ${config.sourceType}")
    }
  }
}
