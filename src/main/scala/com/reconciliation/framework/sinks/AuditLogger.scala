package com.reconciliation.framework

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class AuditLog(
    jobName: String,
    runId: String,
    reconType: String,
    status: String,
    message: String,
    timestamp: java.sql.Timestamp
)

object AuditLogger extends Logger {
  def log(spark: SparkSession, config: AppConfig, reconType: String, status: String, message: String): Unit = {
    if (config.auditFlag) {
      val auditLog = Seq(
        AuditLog(
          config.jobName,
          config.runId,
          reconType,
          status,
          message,
          new java.sql.Timestamp(System.currentTimeMillis())
        )
      )
      val auditDf = spark.createDataFrame(auditLog)

      if (config.writeToHiveFlag) {
        auditDf.write.mode(SaveMode.Append).saveAsTable("schema.recon_audit_log")
      } else {
        logger.info(s"AUDIT | ${auditLog.toString()}")
      }
    }
  }
}
