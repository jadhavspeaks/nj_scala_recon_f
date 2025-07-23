-- Sample Insert for Source-to-Target Reconciliation
INSERT INTO RECON_CONFIG (
    jobName, sourceType, sourcePath, sourceTable, sourceQuery,
    targetType, targetPath, targetTable, reconTypes, columnMapping,
    primaryKeyMapping, threshold, businessRuleSQL, notifyOnSuccess,
    notifyOnFailure, emailTo, emailSubject, writeToHiveFlag,
    auditFlag, logLevel, runId, createdBy, runDate
) VALUES (
    'source_to_target_recon', 'Local', '/path/to/source/data.csv', NULL, NULL,
    'Hive', NULL, 'schema.target_table', 'source_to_target,record_count', 'id:emp_id,name:emp_name,salary:emp_salary',
    'id:emp_id', NULL, NULL, 'Y',
    'Y', 'user@example.com', 'Reconciliation Report for source_to_target_recon', 'Y',
    'Y', 'INFO', 'run-12345', 'Jules', SYSDATE
);

-- Sample Insert for Business Rule Reconciliation
INSERT INTO RECON_CONFIG (
    jobName, sourceType, sourcePath, sourceTable, sourceQuery,
    targetType, targetPath, targetTable, reconTypes, columnMapping,
    primaryKeyMapping, threshold, businessRuleSQL, notifyOnSuccess,
    notifyOnFailure, emailTo, emailSubject, writeToHiveFlag,
    auditFlag, logLevel, runId, createdBy, runDate
) VALUES (
    'business_rule_recon', 'Hive', NULL, 'schema.source_table', NULL,
    'Local', '/path/to/target/data.parquet', NULL, 'business_rule,missing_extra_records', NULL,
    'order_id:order_id', 0.05, 'SELECT * FROM schema.source_table WHERE amount > 1000', 'Y',
    'Y', 'user@example.com', 'Reconciliation Report for business_rule_recon', 'Y',
    'Y', 'DEBUG', 'run-67890', 'Jules', SYSDATE
);
