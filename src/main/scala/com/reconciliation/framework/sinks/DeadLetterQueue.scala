package com.reconciliation.framework.sinks

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.reconciliation.framework.core.AppConfig

object DeadLetterQueue extends Logger {
  def write(spark: SparkSession, config: AppConfig, reconType: String, mismatchedRecords: DataFrame): Unit = {
    if (config.auditFlag) {
      DataWriter.write(spark, config, s"dead-letter/$reconType", mismatchedRecords)
    }
  }
}
