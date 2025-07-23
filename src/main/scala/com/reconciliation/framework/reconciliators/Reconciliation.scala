package com.reconciliation.framework.reconciliators

import com.reconciliation.framework.core.AppConfig
import com.reconciliation.framework.readers.DataReader
import com.reconciliation.framework.sinks.{AuditLogger, DeadLetterQueue, Logger}

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reconciliation {
  def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit
}

class SourceToTargetReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Source-to-Target Reconciliation...")
    AuditLogger.log(spark, config, "SourceToTarget", "STARTED", "Starting source to target reconciliation")

    try {
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
          val sourceMismatchCount = sourceMinusTarget.count()
          val targetMismatchCount = targetMinusSource.count()
          val message = s"Mismatches found. $sourceMismatchCount records in source only, $targetMismatchCount records in target only."
        logger.warn(message)
        AuditLogger.log(spark, config, "SourceToTarget", "FAILURE", message)

          if(sourceMismatchCount > 0) {
            DeadLetterQueue.write(spark, config, "SourceToTarget_SourceOnly", sourceMinusTarget)
          }

          if(targetMismatchCount > 0) {
            DeadLetterQueue.write(spark, config, "SourceToTarget_TargetOnly", targetMinusSource)
          }
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
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Business Rule Reconciliation...")
    AuditLogger.log(spark, config, "BusinessRule", "STARTED", "Starting business rule reconciliation")

    try {
      config.businessRuleSQL.foreach { sql =>
        sourceDf.createOrReplaceTempView("source_view")

        val businessRuleResultDf = spark.sql(sql)

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
            DeadLetterQueue.write(spark, config, "BusinessRule_SourceOnly", sourceMinusTarget)
          }

          if(targetMismatchCount > 0) {
            println("Records in target not found in business rule query result:")
            targetMinusSource.show()
            DeadLetterQueue.write(spark, config, "BusinessRule_TargetOnly", targetMinusSource)
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
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Record Count Reconciliation...")
    AuditLogger.log(spark, config, "RecordCount", "STARTED", "Starting record count reconciliation")

    try {
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
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Missing/Extra Records Reconciliation...")
    AuditLogger.log(spark, config, "MissingExtraRecords", "STARTED", "Starting missing/extra records reconciliation")

    try {
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

class ThresholdReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Threshold-based Reconciliation...")
    AuditLogger.log(spark, config, "Threshold", "STARTED", "Starting threshold-based reconciliation")

    try {
        config.threshold.foreach { threshold =>
            val numericColumns = sourceDf.dtypes.filter(_._2 == "DoubleType").map(_._1)

            numericColumns.foreach { colName =>
                val aliasedColName = config.columnMapping.getOrElse(colName, colName)
                if (targetDf.columns.contains(aliasedColName)) {
                    val joinedDf = sourceDf.join(targetDf, sourceDf(colName) === targetDf(aliasedColName))
                    val mismatchedDf = joinedDf.filter(s"abs(${colName} - ${aliasedColName}) > $threshold")

                    if (mismatchedDf.count() > 0) {
                        val message = s"Threshold breached for column $colName"
                        logger.warn(message)
                        AuditLogger.log(spark, config, "Threshold", "FAILURE", message)
                        println(s"Threshold breaches for column $colName:")
                        mismatchedDf.show()
                    } else {
                        val message = s"Threshold check passed for column $colName"
                        logger.info(message)
                        AuditLogger.log(spark, config, "Threshold", "SUCCESS", message)
                    }
                }
            }
        }
    } catch {
      case e: Exception =>
        val message = s"An error occurred during Threshold-based Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "Threshold", "ERROR", message)
    }
  }
}

class SchemaDriftReconciliation extends Reconciliation with Logger {
  override def reconcile(spark: SparkSession, config: AppConfig, sourceDf: DataFrame, targetDf: DataFrame): Unit = {
    logger.info("Performing Schema Drift Reconciliation...")
    AuditLogger.log(spark, config, "SchemaDrift", "STARTED", "Starting schema drift reconciliation")

    try {
      val sourceSchema = sourceDf.schema
      val targetSchema = targetDf.schema

      var driftFound = false
      val driftDetails = new StringBuilder

      if (sourceSchema.length != targetSchema.length) {
        driftFound = true
        driftDetails.append(s"Column count mismatch: Source has ${sourceSchema.length}, Target has ${targetSchema.length}. ")
      }

      val sourceColumns = sourceSchema.fields.map(f => (f.name, f.dataType, f.nullable)).toSeq
      val targetColumns = targetSchema.fields.map(f => (f.name, f.dataType, f.nullable)).toSeq

      if (sourceColumns.map(_._1) != targetColumns.map(_._1)) {
        driftFound = true
        driftDetails.append("Column order mismatch. ")
      }

      val sourceColSet = sourceColumns.map(c => (c._1, c._2.typeName, c._3)).toSet
      val targetColSet = targetColumns.map(c => (c._1, c._2.typeName, c._3)).toSet

      val missingInTarget = sourceColSet -- targetColSet
      val missingInSource = targetColSet -- sourceColSet

      if (missingInTarget.nonEmpty) {
        driftFound = true
        driftDetails.append(s"Columns missing in target: ${missingInTarget.map(_._1).mkString(", ")}. ")
      }

      if (missingInSource.nonEmpty) {
        driftFound = true
        driftDetails.append(s"Columns missing in source: ${missingInSource.map(_._1).mkString(", ")}. ")
      }

      if (driftFound) {
        val message = s"Schema drift detected: ${driftDetails.toString()}"
        logger.warn(message)
        AuditLogger.log(spark, config, "SchemaDrift", "FAILURE", message)
      } else {
        val message = "No schema drift detected."
        logger.info(message)
        AuditLogger.log(spark, config, "SchemaDrift", "SUCCESS", message)
      }

    } catch {
      case e: Exception =>
        val message = s"An error occurred during Schema Drift Reconciliation: ${e.getMessage}"
        logger.error(message, e)
        AuditLogger.log(spark, config, "SchemaDrift", "ERROR", message)
    }
  }
}
