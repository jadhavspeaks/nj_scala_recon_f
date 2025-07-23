package com.reconciliation

import com.reconciliation.logic.ReconLogic
import com.reconciliation.model.ReconConfig
import com.reconciliation.notification.EmailService
import com.reconciliation.reader.DataReader
import com.reconciliation.writer.DataWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {
    val jobName = args(0)

    val spark = SparkSession.builder()
      .appName(s"Data Reconciliation: $jobName")
      .enableHiveSupport()
      .getOrCreate()

    val config = readConfig(jobName)

    val startTime = System.currentTimeMillis()
    var status = "SUCCESS"
    var mismatchCount = 0L

    try {
      val sourceDF = DataReader.read(spark, config)
      val targetDF = if (config.reconType.toLowerCase == "s2t") DataReader.read(spark, config.copy(sourceType = config.targetType, sourceLocation = config.targetLocation)) else null

      val resultDF = config.reconType.toLowerCase match {
        case "s2t" =>
          ReconLogic.s2t(spark, sourceDF, targetDF, config)
        case "sql" =>
          ReconLogic.businessRule(spark, sourceDF, config)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported recon type: ${config.reconType}")
      }

      mismatchCount = resultDF.count()
      if (mismatchCount > 0) {
        status = "FAILURE"
        DataWriter.write(resultDF, config.copy(targetType = "hive", targetLocation = "recon_results"))
      }
    } catch {
      case e: Exception =>
        status = "ERROR"
        e.printStackTrace()
    } finally {
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000

      val emailBody = generateEmailBody(config, status, mismatchCount, totalTime)
      if ((status == "SUCCESS" && config.notifyOnSuccess == "Y") || (status != "SUCCESS" && config.notifyOnFailure == "Y")) {
        EmailService.sendEmail(config.emailTo, config.emailCc, s"Reconciliation Report: ${config.jobName} - $status", emailBody)
      }

      spark.stop()
    }
  }

  def readConfig(jobName: String): ReconConfig = {
    // Replace with your Oracle connection details
    val url = "jdbc:oracle:thin:@//your-oracle-host:1521/your-service"
    val username = "your-username"
    val password = "your-password"

    val query = s"SELECT * FROM recon_config WHERE job_name = '$jobName'"

    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val rs = statement.executeQuery(query)
      if (rs.next()) {
        ReconConfig(
          rs.getString("job_name"),
          rs.getString("source_type"),
          rs.getString("source_location"),
          rs.getString("target_type"),
          rs.getString("target_location"),
          rs.getString("column_mapping"),
          rs.getString("key_mapping"),
          rs.getString("recon_type"),
          rs.getString("sql_query"),
          rs.getString("email_to"),
          rs.getString("email_cc"),
          rs.getString("notify_on_success"),
          rs.getString("notify_on_failure")
        )
      } else {
        throw new RuntimeException(s"No configuration found for job: $jobName")
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def generateEmailBody(config: ReconConfig, status: String, mismatchCount: Long, totalTime: Long): String = {
    s"""
      |<html>
      |<body>
      |<h2>Reconciliation Report for ${config.jobName}</h2>
      |<table border="1">
      |  <tr>
      |    <td>Job Name</td>
      |    <td>${config.jobName}</td>
      |  </tr>
      |  <tr>
      |    <td>Status</td>
      |    <td style="color:${if (status == "SUCCESS") "green" else "red"};">${status}</td>
      |  </tr>
      |  <tr>
      |    <td>Mismatch Count</td>
      |    <td>${mismatchCount}</td>
      |  </tr>
      |  <tr>
      |    <td>Total Time (seconds)</td>
      |    <td>${totalTime}</td>
      |  </tr>
      |</table>
      |</body>
      |</html>
    """.stripMargin
  }
}
