package com.reconciliation.writer

import com.reconciliation.model.ReconConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWriter {

  def write(df: DataFrame, config: ReconConfig): Unit = {
    config.targetType.toLowerCase match {
      case "hive" =>
        df.write
          .mode(SaveMode.Append)
          .saveAsTable(config.targetLocation)
      case "parquet" =>
        df.write
          .mode(SaveMode.Overwrite)
          .parquet(config.targetLocation)
      case "orc" =>
        df.write
          .mode(SaveMode.Overwrite)
          .orc(config.targetLocation)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported target type: ${config.targetType}")
    }
  }
}
