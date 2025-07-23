CREATE TABLE recon_config (
    job_name VARCHAR2(100) PRIMARY KEY,
    source_type VARCHAR2(20),
    source_location VARCHAR2(200),
    target_type VARCHAR2(20),
    target_location VARCHAR2(200),
    column_mapping VARCHAR2(1000),
    key_mapping VARCHAR2(200),
    recon_type VARCHAR2(20),
    sql_query VARCHAR2(4000),
    email_to VARCHAR2(200),
    email_cc VARCHAR2(200),
    notify_on_success CHAR(1),
    notify_on_failure CHAR(1)
);
