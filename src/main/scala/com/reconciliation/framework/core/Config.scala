package com.reconciliation.framework.core

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AppConfig(
    jobName: String,
    sourceType: String,
    sourcePath: Option[String],
    sourceTable: Option[String],
    sourceQuery: Option[String],
    targetType: String,
    targetPath: Option[String],
    targetTable: Option[String],
    reconTypes: Seq[String],
    columnMapping: Map[String, String],
    primaryKeyMapping: Map[String, String],
    threshold: Option[Double],
    businessRuleSQL: Option[String],
    notifyOnSuccess: Boolean,
    notifyOnFailure: Boolean,
    emailTo: Option[String],
    emailSubject: Option[String],
    writeToHiveFlag: Boolean,
    auditFlag: Boolean,
    logLevel: String,
    runId: String,
    createdBy: String,
    runDate: java.sql.Date
)

object Config {
  def load(jobName: String): AppConfig = {
    // This is a placeholder for the actual database connection logic.
    // In a real application, you would use JDBC to connect to Oracle.
    // For now, we will use the sample data we created earlier.
    val jdbcUrl = "jdbc:oracle:thin:@//localhost:1521/ORCL"
    val username = "your_username"
    val password = "your_password"

    var connection: java.sql.Connection = null
    var statement: java.sql.PreparedStatement = null
    var resultSet: java.sql.ResultSet = null

    try {
      // Class.forName("oracle.jdbc.driver.OracleDriver")
      // connection = java.sql.DriverManager.getConnection(jdbcUrl, username, password)
      // val query = "SELECT * FROM RECON_CONFIG WHERE jobName = ?"
      // statement = connection.prepareStatement(query)
      // statement.setString(1, jobName)
      // resultSet = statement.executeQuery()

      // if (resultSet.next()) {
        // AppConfig(
          // jobName = resultSet.getString("jobName"),
          // sourceType = resultSet.getString("sourceType"),
          // sourcePath = Option(resultSet.getString("sourcePath")),
          // sourceTable = Option(resultSet.getString("sourceTable")),
          // sourceQuery = Option(resultSet.getString("sourceQuery")),
          // targetType = resultSet.getString("targetType"),
          // targetPath = Option(resultSet.getString("targetPath")),
          // targetTable = Option(resultSet.getString("targetTable")),
          // reconTypes = resultSet.getString("reconTypes").split(",").map(_.trim),
          // columnMapping = parseMapping(resultSet.getString("columnMapping")),
          // primaryKeyMapping = parseMapping(resultSet.getString("primaryKeyMapping")),
          // threshold = Option(resultSet.getBigDecimal("threshold")).map(_.doubleValue()),
          // businessRuleSQL = Option(resultSet.getString("businessRuleSQL")),
          // notifyOnSuccess = resultSet.getString("notifyOnSuccess") == "Y",
          // notifyOnFailure = resultSet.getString("notifyOnFailure") == "Y",
          // emailTo = Option(resultSet.getString("emailTo")),
          // emailSubject = Option(resultSet.getString("emailSubject")),
          // writeToHiveFlag = resultSet.getString("writeToHiveFlag") == "Y",
          // auditFlag = resultSet.getString("auditFlag") == "Y",
          // logLevel = resultSet.getString("logLevel"),
          // runId = resultSet.getString("runId"),
          // createdBy = resultSet.getString("createdBy"),
          // runDate = resultSet.getDate("runDate")
        // )
      // } else {
        // throw new RuntimeException(s"No configuration found for job: $jobName")
      // }

      // Dummy config for now
      createDummyConfig(jobName)

    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  private def parseMapping(mapping: String): Map[String, String] = {
    if (mapping == null || mapping.isEmpty) {
      Map.empty
    } else {
      mapping.split(",").map { pair =>
        val Array(key, value) = pair.split(":")
        key.trim -> value.trim
      }.toMap
    }
  }

  private def createDummyConfig(jobName: String): AppConfig = {
    if (jobName == "source_to_target_recon") {
      AppConfig(
        jobName = "source_to_target_recon",
        sourceType = "Local",
        sourcePath = Some("src/main/resources/source_to_target_recon_source.csv"),
        sourceTable = None,
        sourceQuery = None,
        targetType = "Hive",
        targetPath = None,
        targetTable = Some("schema.target_table"),
        reconTypes = Seq("source_to_target", "record_count"),
        columnMapping = Map("id" -> "emp_id", "name" -> "emp_name", "salary" -> "emp_salary"),
        primaryKeyMapping = Map("id" -> "emp_id"),
        threshold = None,
        businessRuleSQL = None,
        notifyOnSuccess = true,
        notifyOnFailure = true,
        emailTo = Some("user@example.com"),
        emailSubject = Some("Reconciliation Report for source_to_target_recon"),
        writeToHiveFlag = true,
        auditFlag = true,
        logLevel = "INFO",
        runId = "run-12345",
        createdBy = "Jules",
        runDate = new java.sql.Date(System.currentTimeMillis())
      )
    } else {
      AppConfig(
        jobName = "business_rule_recon",
        sourceType = "Hive",
        sourcePath = None,
        sourceTable = Some("schema.source_table"),
        sourceQuery = None,
        targetType = "Local",
        targetPath = Some("src/main/resources/business_rule_recon_target.parquet"),
        targetTable = None,
        reconTypes = Seq("business_rule", "missing_extra_records"),
        columnMapping = Map.empty,
        primaryKeyMapping = Map("order_id"-> "order_id"),
        threshold = Some(0.05),
        businessRuleSQL = Some("SELECT * FROM source_view WHERE amount > 1000"),
        notifyOnSuccess = true,
        notifyOnFailure = true,
        emailTo = Some("user@example.com"),
        emailSubject = Some("Reconciliation Report for business_rule_recon"),
        writeToHiveFlag = true,
        auditFlag = true,
        logLevel = "DEBUG",
        runId = "run-67890",
        createdBy = "Jules",
        runDate = new java.sql.Date(System.currentTimeMillis())
      )
    }
  }
}
