package com.reconciliation.framework.reconciliators

import com.reconciliation.framework.core.AppConfig
import com.reconciliation.framework.readers.DataReader
import com.reconciliation.framework.sinks.{AuditLogger, Logger}

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reconciliation {
  def reconcile(spark: SparkSession, config: AppConfig): Unit
}

class SourceToTargetReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
    logger.info("Performing Source-to-Target Reconciliation...")
    AuditLogger.log(spark, config, "SourceToTarget", "STARTED", "Starting source to target reconciliation")

    try {
      val sourceDf = DataReader.read(spark, config.sourceType, config.sourcePath, config.sourceTable, config.sourceQuery)
      val targetDf = DataReader.read(spark, config.targetType, config.targetPath, config.targetTable, None)

      val aliasedSourceDf = config.columnMapping.foldLeft(sourceDf) { (df, mapping) =>
        df.withColumnRenamed(mapping._1, mapping._2)
      }

      val sourceColumns = aliasedSourceDf.columns.sorted
      val targetColumns = targetDf.columns.sorted

      if (sourceColumns.deep != targetColumns.deep) {
        val message = "Schema Drift Detected: Columns do not match"
        logger.warn(message)
        AuditLogger.log(spark, config, "SourceToTarget", "WARNING", message)
      }

      val sourceMinusTarget = aliasedSourceDf.exceptAll(targetDf)
      val targetMinusSource = targetDf.exceptAll(aliasedSourceDf)

      if (sourceMinusTarget.count() == 0 && targetMinusSource.count() == 0) {
        val message = "Source and Target are perfectly matched."
        logger.info(message)
        AuditLogger.log(spark, config, "SourceToTarget", "SUCCESS", message)
      } else {
        val message = s"Mismatches found. ${sourceMinusTarget.count()} records in source only, ${targetMinusSource.count()} records in target only."
        logger.warn(message)
        AuditLogger.log(spark, config, "SourceToTarget", "FAILURE", message)
        // In a real application, you would write the mismatched records to a file or table.
      }
    } catch {
      case e: Exception =>
        val message = s"An error occurred during Source-to-Target Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "SourceToTarget", "ERROR", message)
    }
  }
}


class BusinessRuleReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
    logger.info("Performing Business Rule Reconciliation...")
    AuditLogger.log(spark, config, "BusinessRule", "STARTED", "Starting business rule reconciliation")

    try {
      config.businessRuleSQL.foreach { sql =>
        val sourceDf = DataReader.read(spark, config.sourceType, config.sourcePath, config.sourceTable, config.sourceQuery)
        sourceDf.createOrReplaceTempView("source_view")

        val businessRuleResultDf = spark.sql(sql)

        val targetDf = DataReader.read(spark, config.targetType, config.targetPath, config.targetTable, None)

        val sourceMinusTarget = businessRuleResultDf.exceptAll(targetDf)
        val targetMinusSource = targetDf.exceptAll(businessRuleResultDf)

        if (sourceMinusTarget.count() == 0 && targetMinusSource.count() == 0) {
          val message = "Business rule validation passed."
          logger.info(message)
          AuditLogger.log(spark, config, "BusinessRule", "SUCCESS", message)
        } else {
          val sourceMismatchCount = sourceMinusTarget.count()
          val targetMismatchCount = targetMinusSource.count()
          val message = s"Business rule validation failed. $sourceMismatchCount records in source query result not in target, and $targetMismatchCount records in target not in source query result."
          logger.warn(message)
          AuditLogger.log(spark, config, "BusinessRule", "FAILURE", message)

          if(sourceMismatchCount > 0) {
            println("Records from business rule query not found in target:")
            sourceMinusTarget.show()
          }

          if(targetMismatchCount > 0) {
            println("Records in target not found in business rule query result:")
            targetMinusSource.show()
          }
        }
      }
    } catch {
      case e: Exception =>
        val message = s"An error occurred during Business Rule Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "BusinessRule", "ERROR", message)
    }
  }
}

class RecordCountReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
    logger.info("Performing Record Count Reconciliation...")
    AuditLogger.log(spark, config, "RecordCount", "STARTED", "Starting record count reconciliation")

    try {
      val sourceDf = DataReader.read(spark, config.sourceType, config.sourcePath, config.sourceTable, config.sourceQuery)
      val targetDf = DataReader.read(spark, config.targetType, config.targetPath, config.targetTable, None)

      val sourceCount = sourceDf.count()
      val targetCount = targetDf.count()

      if (sourceCount == targetCount) {
        val message = s"Record counts match: $sourceCount"
        logger.info(message)
        AuditLogger.log(spark, config, "RecordCount", "SUCCESS", message)
      } else {
        val message = s"Record count mismatch: Source has $sourceCount records, Target has $targetCount records."
        logger.warn(message)
        AuditLogger.log(spark, config, "RecordCount", "FAILURE", message)
      }
    } catch {
      case e: Exception =>
        val message = s"An error occurred during Record Count Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "RecordCount", "ERROR", message)
    }
  }
}

class MissingExtraRecordsReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
    logger.info("Performing Missing/Extra Records Reconciliation...")
    AuditLogger.log(spark, config, "MissingExtraRecords", "STARTED", "Starting missing/extra records reconciliation")

    try {
      val sourceDf = DataReader.read(spark, config.sourceType, config.sourcePath, config.sourceTable, config.sourceQuery)
      val targetDf = DataReader.read(spark, config.targetType, config.targetPath, config.targetTable, None)

      val sourcePk = config.primaryKeyMapping.keys.toSeq
      val targetPk = config.primaryKeyMapping.values.toSeq

      val aliasedSourceDf = config.primaryKeyMapping.foldLeft(sourceDf) { (df, mapping) =>
        df.withColumnRenamed(mapping._1, mapping._2)
      }

      val sourceKeys = aliasedSourceDf.select(targetPk.head, targetPk.tail: _*)
      val targetKeys = targetDf.select(targetPk.head, targetPk.tail: _*)

      val missingInTarget = sourceKeys.except(targetKeys)
      val missingInSource = targetKeys.except(sourceKeys)

      if (missingInTarget.count() == 0 && missingInSource.count() == 0) {
        val message = "All records are present in both source and target."
        logger.info(message)
        AuditLogger.log(spark, config, "MissingExtraRecords", "SUCCESS", message)
      } else {
        val message = s"Missing/Extra records found. ${missingInTarget.count()} missing in target, ${missingInSource.count()} missing in source."
        logger.warn(message)
        AuditLogger.log(spark, config, "MissingExtraRecords", "FAILURE", message)
        // In a real application, you would write the missing/extra keys to a file or table.
      }
    } catch {
      case e: Exception =>
        val message = s"An error occurred during Missing/Extra Records Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "MissingExtraRecords", "ERROR", message)
    }
  }
}

class ThresholdReconciliation extends Reconciliation {
    override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
        println("Performing Threshold-based Reconciliation...")
        // Implementation details will be added later
    }
}

class SchemaDriftReconciliation extends Reconciliation {
    override def reconcile(spark: SparkSession, config: AppConfig): Unit = {
        println("Performing Schema Drift Reconciliation...")
        // Implementation details will be added later
    }
}
