package com.reconciliation.framework.sinks

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.reconciliation.framework.core.AppConfig

object DeadLetterQueue extends Logger {
  def write(spark: SparkSession, config: AppConfig, reconType: String, mismatchedRecords: DataFrame): Unit = {
    if (config.auditFlag) {
      val path = s"/path/to/dead-letter-queue/${config.jobName}/${config.runId}/$reconType"
      logger.info(s"Writing mismatched records to dead-letter queue: $path")
      mismatchedRecords.write.mode(SaveMode.Overwrite).parquet(path)
    }
  }
}
