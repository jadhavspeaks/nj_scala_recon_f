package com.reconciliation.framework.core

import com.reconciliation.framework.reconciliators.ReconciliationFactory
import com.reconciliation.framework.services.EmailService
import com.reconciliation.framework.readers.DataReader

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val jobName = args(0)

    val spark = SparkSession.builder()
      .appName(s"Data Reconciliation Framework - $jobName")
      .enableHiveSupport()
      .getOrCreate()

    val config = Config.load(jobName)

    val sourceDf = DataReader.read(spark, config.sourceType, config.sourcePath, config.sourceTable, config.sourceQuery, config.delimiter)
    val targetDf = DataReader.read(spark, config.targetType, config.targetPath, config.targetTable, None, None)

    val reconciliations = ReconciliationFactory.getReconciliations(config)

    reconciliations.foreach(_.reconcile(spark, config, sourceDf, targetDf))

    // This is a placeholder for the actual email sending logic.
    // In a real application, you would gather the results from the audit table.
    val auditDf = spark.read.table("schema.recon_audit_log").filter(s"runId = '${config.runId}'")
    val results = auditDf.collect()

    val (subject, body) = if (results.exists(_.getAs[String]("status") == "FAILURE")) {
        (s"FAILURE: ${config.emailSubject.get}", createFailureReport(config, results))
    } else {
        (s"SUCCESS: ${config.emailSubject.get}", createSuccessReport(config, results))
    }

    EmailService.sendEmail(config, subject, body)

    spark.stop()
  }

  private def createSuccessReport(config: AppConfig, results: Array[org.apache.spark.sql.Row]): String = {
    val resultsTable = results.map(row => s"<tr><td>${row.getAs[String]("reconType")}</td><td>${row.getAs[String]("status")}</td><td>${row.getAs[String]("message")}</td></tr>").mkString

    s"""
      |<html>
      |<body>
      |<h1>Reconciliation Report for ${config.jobName}</h1>
      |<h2>Status: SUCCESS</h2>
      |<table border="1" style="width:100%">
      |<tr><th>Recon Type</th><th>Status</th><th>Message</th></tr>
      |$resultsTable
      |</table>
      |</body>
      |</html>
    """.stripMargin
  }

  private def createFailureReport(config: AppConfig, results: Array[org.apache.spark.sql.Row]): String = {
    val resultsTable = results.map(row => s"<tr><td>${row.getAs[String]("reconType")}</td><td>${row.getAs[String]("status")}</td><td>${row.getAs[String]("message")}</td></tr>").mkString

    s"""
      |<html>
      |<body>
      |<h1>Reconciliation Report for ${config.jobName}</h1>
      |<h2>Status: FAILURE</h2>
      |<table border="1" style="width:100%">
      |<tr><th>Recon Type</th><th>Status</th><th>Message</th></tr>
      |$resultsTable
      |</table>
      |</body>
      |</html>
    """.stripMargin
  }
}
