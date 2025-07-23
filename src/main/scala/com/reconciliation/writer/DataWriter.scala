package com.reconciliation.writer

import com.reconciliation.model.ReconConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object DataWriter {

  def write(df: DataFrame, config: ReconConfig, status: String, startTime: Long, endTime: Long): Unit = {
    val totalTime = (endTime - startTime) / 1000
    val recordCount = df.count()

    val resultDF = df
      .withColumn("job_name", lit(config.jobName))
      .withColumn("recon_type", lit(config.reconType))
      .withColumn("status", lit(status))
      .withColumn("start_time", to_timestamp(lit(startTime / 1000)))
      .withColumn("end_time", to_timestamp(lit(endTime / 1000)))
      .withColumn("total_time_taken_for_execution", lit(totalTime))
      .withColumn("record_count", lit(recordCount))
      .withColumn("mismatch_count", when(col("mismatch_details").isNotNull, 1).otherwise(0))

    config.targetType.toLowerCase match {
      case "hive" =>
        resultDF.write
          .mode(SaveMode.Append)
          .saveAsTable(config.targetLocation)
      case "parquet" =>
        resultDF.write
          .mode(SaveMode.Overwrite)
          .parquet(config.targetLocation)
      case "orc" =>
        resultDF.write
          .mode(SaveMode.Overwrite)
          .orc(config.targetLocation)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported target type: ${config.targetType}")
    }
  }
}
