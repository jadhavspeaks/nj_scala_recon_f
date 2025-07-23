package com.reconciliation.framework.core

import com.reconciliation.framework.reconciliators.ReconciliationFactory
import com.reconciliation.framework.services.EmailService
import com.reconciliation.framework.readers.DataReader

import org.apache.spark.sql.SparkSession
import scala.io.Source

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

    spark.stop()
  }
}
