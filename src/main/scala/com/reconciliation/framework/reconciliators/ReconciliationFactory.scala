package com.reconciliation.framework.reconciliators

import com.reconciliation.framework.core.AppConfig

object ReconciliationFactory {
  def getReconciliations(config: AppConfig): Seq[Reconciliation] = {
    config.reconTypes.map {
      case "source_to_target" => new SourceToTargetReconciliation()
      case "business_rule" => new BusinessRuleReconciliation()
      case "record_count" => new RecordCountReconciliation()
      case "missing_extra_records" => new MissingExtraRecordsReconciliation()
      case "threshold" => new ThresholdReconciliation()
      case "schema_drift" => new SchemaDriftReconciliation()
      case _ => throw new IllegalArgumentException("Unknown reconciliation type")
    }
  }
}
