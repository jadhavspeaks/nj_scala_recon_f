package com.reconciliation.framework.core

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AppConfig(
    jobName: String,
    sourceType: String,
    sourcePath: Option[String],
    sourceTable: Option[String],
    sourceQuery: Option[String],
    sourceDelimiter: Option[String],
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
    emailSmtpHost: String,
    emailSmtpPort: Int,
    emailUsername: Option[String],
    emailPassword: Option[String],
    writeToHiveFlag: Boolean,
    auditFlag: Boolean,
    logLevel: String,
    runId: String,
    createdBy: String,
                   runDate: String
)
object Config {
  def load(jobName: String): AppConfig = {
    // This is a placeholder for the actual database connection logic.
    // In a real application, you would use JDBC to connect to Oracle.
    val jdbcUrl = "jdbc:oracle:thin:@//localhost:1521/ORCL"
    val username = "your_username"
    val password = "your_password"

    var connection: java.sql.Connection = null
    var statement: java.sql.PreparedStatement = null
    var resultSet: java.sql.ResultSet = null

    var appConfig: Option[AppConfig] = None
    var retries = 3
    while (appConfig.isEmpty && retries > 0) {
      try {
        Class.forName("oracle.jdbc.driver.OracleDriver")
        connection = java.sql.DriverManager.getConnection(jdbcUrl, username, password)
        val query = "SELECT * FROM RECON_CONFIG WHERE jobName = ?"
        statement = connection.prepareStatement(query)
        statement.setString(1, jobName)
        resultSet = statement.executeQuery()

        if (resultSet.next()) {
          appConfig = Some(AppConfig(
            jobName = resultSet.getString("jobName"),
            sourceType = resultSet.getString("sourceType"),
            sourcePath = Option(resultSet.getString("sourcePath")),
            sourceTable = Option(resultSet.getString("sourceTable")),
            sourceQuery = Option(resultSet.getString("sourceQuery")),
            sourceDelimiter = Option(resultSet.getString("sourceDelimiter")),
            targetType = resultSet.getString("targetType"),
            targetPath = Option(resultSet.getString("targetPath")),
            targetTable = Option(resultSet.getString("targetTable")),
            reconTypes = resultSet.getString("reconTypes").split(",").map(_.trim),
            columnMapping = parseMapping(resultSet.getString("columnMapping")),
            primaryKeyMapping = parseMapping(resultSet.getString("primaryKeyMapping")),
            threshold = Option(resultSet.getBigDecimal("threshold")).map(_.doubleValue()),
            businessRuleSQL = Option(resultSet.getString("businessRuleSQL")),
            notifyOnSuccess = resultSet.getString("notifyOnSuccess") == "Y",
            notifyOnFailure = resultSet.getString("notifyOnFailure") == "Y",
            emailTo = Option(resultSet.getString("emailTo")),
            emailSubject = Option(resultSet.getString("emailSubject")),
            emailSmtpHost = resultSet.getString("emailSmtpHost"),
            emailSmtpPort = resultSet.getInt("emailSmtpPort"),
            emailUsername = Option(resultSet.getString("emailUsername")),
            emailPassword = Option(resultSet.getString("emailPassword")),
            writeToHiveFlag = resultSet.getString("writeToHiveFlag") == "Y",
            auditFlag = resultSet.getString("auditFlag") == "Y",
            logLevel = resultSet.getString("logLevel"),
            runId = resultSet.getString("runId"),
            createdBy = resultSet.getString("createdBy"),
            runDate = resultSet.getString("runDate"),
            delimiter = Option(resultSet.getString("delimiter"))
          ))
        } else {
          throw new RuntimeException(s"No configuration found for job: $jobName")
        }
      } catch {
        case e: Exception =>
          retries -= 1
          println(s"Failed to connect to the database. Retries left: $retries. Error: ${e.getMessage}")
          Thread.sleep(5000)
      } finally {
        if (resultSet != null) resultSet.close()
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }
    appConfig.getOrElse(throw new RuntimeException("Failed to load configuration after multiple retries."))
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
}
