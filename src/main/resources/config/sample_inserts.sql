-- Sample insert for Source-to-Target reconciliation
INSERT INTO recon_config (job_name, source_type, source_location, target_type, target_location, column_mapping, key_mapping, recon_type, sql_query, email_to, email_cc, notify_on_success, notify_on_failure)
VALUES ('s2t_customer_recon', 'csv', '/path/to/source/customer.csv', 'csv', '/path/to/target/customer.csv', 'id:cust_id,name:cust_name,email:cust_email', 'id', 'S2T', NULL, 'user@example.com', NULL, 'Y', 'Y');

-- Sample insert for Business Rule reconciliation
INSERT INTO recon_config (job_name, source_type, source_location, target_type, target_location, column_mapping, key_mapping, recon_type, sql_query, email_to, email_cc, notify_on_success, notify_on_failure)
VALUES ('business_rule_order_check', 'hive', 'sales.orders', NULL, NULL, NULL, NULL, 'SQL', 'SELECT * FROM source_view WHERE order_status = ''PENDING'' AND order_date < (SYSDATE - 7)', 'user@example.com', 'manager@example.com', 'N', 'Y');
