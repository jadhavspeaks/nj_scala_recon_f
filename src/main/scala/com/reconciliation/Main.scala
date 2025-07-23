package com.reconciliation

import com.reconciliation.logic.ReconLogic
import com.reconciliation.model.ReconConfig
import com.reconciliation.notification.EmailService
import com.reconciliation.reader.DataReader
import com.reconciliation.writer.DataWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.{Connection, DriverManager}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.config.Configurator
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    val jobName = args(0)

    // Set up logging
    val logDir = s"logs/$jobName"
    new File(logDir).mkdirs()
    System.setProperty("log.dir", logDir)
    val logger = LogManager.getLogger(this.getClass.getName)


    val spark = SparkSession.builder()
      .appName(s"Data Reconciliation: $jobName")
      .enableHiveSupport()
      .getOrCreate()

    logger.info(s"Starting reconciliation job: $jobName")

    val config = readConfig(jobName)
    logger.info(s"Configuration loaded for job: $jobName")

    val startTime = System.currentTimeMillis()
    var status = "SUCCESS"
    var mismatchCount = 0L
    var resultDF: DataFrame = null

    try {
      val sourceDF = DataReader.read(spark, config)
      logger.info("Source data loaded successfully.")
      val targetDF = if (config.reconType.toLowerCase == "s2t") DataReader.read(spark, config.copy(sourceType = config.targetType, sourceLocation = config.targetLocation)) else null
      if (targetDF != null) logger.info("Target data loaded successfully.")


      resultDF = config.reconType.toLowerCase match {
        case "s2t" =>
          ReconLogic.s2t(spark, sourceDF, targetDF, config)
        case "sql" =>
          ReconLogic.businessRule(spark, sourceDF, config)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported recon type: ${config.reconType}")
      }
      logger.info("Reconciliation logic executed.")


      mismatchCount = resultDF.count()
      if (mismatchCount > 0) {
        status = "FAILURE"
        logger.warn(s"Mismatch found. Count: $mismatchCount")
        DataWriter.write(resultDF, config.copy(targetType = "hive", targetLocation = "recon_results"), status, startTime, System.currentTimeMillis())
        logger.info("Mismatch results written to Hive.")
      } else {
        logger.info("No mismatches found.")
      }
    } catch {
      case e: Exception =>
        status = "ERROR"
        logger.error(s"An error occurred during reconciliation: ${e.getMessage}", e)
    } finally {
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000
      logger.info(s"Reconciliation finished with status: $status. Total time: $totalTime seconds.")


      if ((status == "SUCCESS" && config.notifyOnSuccess == "Y") || (status != "SUCCESS" && config.notifyOnFailure == "Y")) {
        EmailService.sendEmail(config, status, mismatchCount, totalTime, resultDF)
        logger.info("Email notification sent.")
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
}
