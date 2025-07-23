package com.reconciliation.framework.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {

  private val cache = scala.collection.mutable.Map[String, DataFrame]()

  def read(spark: SparkSession, sourceType: String, path: Option[String], table: Option[String], query: Option[String]): DataFrame = {
    val cacheKey = s"$sourceType-$path-$table-$query"
    cache.getOrElseUpdate(cacheKey, {
        val df = sourceType.toLowerCase match {
        case "local" =>
            val format = path.get.split('.').last
            spark.read.format(format).option("header", "true").load(path.get)
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
