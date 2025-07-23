package com.reconciliation.framework.sinks

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.reconciliation.framework.core.AppConfig

object DataWriter extends Logger {
  def write(spark: SparkSession, config: AppConfig, reconType: String, data: DataFrame): Unit = {
    val path = s"/path/to/output/${config.jobName}/${config.runId}/$reconType"
    logger.info(s"Writing data to $path")
    data.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
