package com.reconciliation.logic

import com.reconciliation.model.ReconConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ReconLogic {

  def s2t(spark: SparkSession, sourceDF: DataFrame, targetDF: DataFrame, config: ReconConfig): DataFrame = {
    val keyColumns = config.keyMapping.split(",").map(_.trim)
    val columnMapping = config.columnMapping.split(",").map { mapping =>
      val parts = mapping.split(":").map(_.trim)
      (parts(0), parts(1))
    }.toMap

    val aliasedTargetDF = columnMapping.foldLeft(targetDF) { (df, mapping) =>
      df.withColumnRenamed(mapping._2, mapping._1)
    }

    val joinCondition = keyColumns.map(c => sourceDF(c) === aliasedTargetDF(c)).reduce(_ && _)
    val joinedDF = sourceDF.join(aliasedTargetDF, joinCondition, "full_outer")

    val comparisonColumns = columnMapping.keys.filterNot(keyColumns.contains)

    val mismatchDF = joinedDF.withColumn("mismatch_details",
      concat_ws(",",
        comparisonColumns.map(c =>
          when(sourceDF(c) =!= aliasedTargetDF(c) || (sourceDF(c).isNotNull && aliasedTargetDF(c).isNull) || (sourceDF(c).isNull && aliasedTargetDF(c).isNotNull),
            concat(lit(s"$c:"), sourceDF(c), lit("!="), aliasedTargetDF(c)))
        ).toSeq: _*
      )
    ).filter(col("mismatch_details").notEqual(""))

    mismatchDF
  }

  def businessRule(spark: SparkSession, sourceDF: DataFrame, config: ReconConfig): DataFrame = {
    sourceDF.createOrReplaceTempView("source_view")
    spark.sql(config.sqlQuery)
  }
}
