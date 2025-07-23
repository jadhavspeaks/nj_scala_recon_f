package com.reconciliation.framework.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  private val cache = scala.collection.mutable.Map[String, DataFrame]()

  def read(spark: SparkSession, sourceType: String, path: Option[String], table: Option[String], query: Option[String], delimiter: Option[String]): DataFrame = {
    val cacheKey = s"$sourceType-$path-$table-$query-$delimiter"
    cache.getOrElseUpdate(cacheKey, {
        val df = sourceType.toLowerCase match {
        case "local" =>
            val format = path.get.split('.').last
            val reader = spark.read.format(format).option("header", "true")
            delimiter.foreach(d => reader.option("delimiter", d))
            reader.load(path.get)
        case "hive" =>
            spark.table(table.get)
        case "jdbc" =>
            // This is a placeholder for the actual JDBC read logic.
            // You would need to configure the JDBC connection properties.
            spark.emptyDataFrame
        case _ =>
            throw new IllegalArgumentException(s"Unsupported source type: $sourceType")
        }
        df.cache()
        df
    })
  }
}
