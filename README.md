# Data Reconciliation Framework

This is a config-driven data reconciliation framework built using Maven, Scala, and Spark.

## Architecture

The framework is designed to be modular and extensible. The core components are:
- **Main Application**: The entry point of the application, responsible for reading the configuration, orchestrating the reconciliation process, and sending notifications.
- **DataReader**: A component for reading data from various sources like CSV, Parquet, ORC, and Hive.
- **ReconLogic**: Contains the business logic for the two types of reconciliation: Source-to-Target (S2T) and Business Rule (SQL).
- **DataWriter**: A component for writing the reconciliation results to a Hive table or files.
- **EmailService**: A utility for sending HTML email notifications.
- **ReconConfig**: A case class that represents the configuration for a reconciliation job.

## How to Build

To build the project, run the following command from the root directory:

```bash
mvn clean package
```

## How to Run

To run a reconciliation job, use the `spark-submit` command:

```bash
spark-submit \
  --class com.reconciliation.Main \
  --master <your-spark-master> \
  --jars /path/to/ojdbc8.jar \
  target/data-reconciliation-framework-1.0.0.jar <job-name>
```

Replace `<your-spark-master>` with your Spark master URL, `/path/to/ojdbc8.jar` with the actual path to the Oracle JDBC driver, and `<job-name>` with the name of the job you want to run (as defined in the `recon_config` table).

## Configuration

The entire reconciliation process is driven by a configuration stored in an Oracle table named `recon_config`. Each row in this table represents a single reconciliation job.

### `recon_config` Table Schema

| Column | Data Type | Description |
| --- | --- | --- |
| `job_name` | VARCHAR2(100) | The primary key for the table. Each job has a unique name. |
| `source_type` | VARCHAR2(20) | The type of the source data. Supported values: `csv`, `excel`, `text`, `dat`, `parquet`, `orc`, `hive`. |
| `source_location` | VARCHAR2(200) | The location of the source data. For file-based sources, this is the file path. For Hive, this is the table name (`schema.table`). |
| `target_type` | VARCHAR2(20) | The type of the target data. Supported values: `hive`, `parquet`, `orc`. |
| `target_location` | VARCHAR2(200) | The location of the target data. For Hive, this is the table name. For file-based targets, this is the directory path. |
| `column_mapping` | VARCHAR2(1000) | A comma-separated list of column mappings for S2T reconciliation. Format: `source_col1:target_col1,source_col2:target_col2`. |
| `key_mapping` | VARCHAR2(200) | A comma-separated list of key columns used for joining the source and target dataframes. |
| `recon_type` | VARCHAR2(20) | The type of reconciliation to perform. Supported values: `S2T`, `SQL`. |
| `sql_query` | VARCHAR2(4000) | The SQL query to execute for Business Rule reconciliation. The source dataframe is registered as a temporary view named `source_view`. |
| `email_to` | VARCHAR2(200) | A comma-separated list of email addresses to send the notification to. |
| `email_cc` | VARCHAR2(200) | A comma-separated list of email addresses to CC on the notification. |
| `notify_on_success` | CHAR(1) | A flag to indicate whether to send a notification on successful completion. `Y` for yes, `N` for no. |
| `notify_on_failure` | CHAR(1) | A flag to indicate whether to send a notification on failure. `Y` for yes, `N` for no. |

### Sample Configurations

**Source-to-Target (S2T) Reconciliation**

```sql
INSERT INTO recon_config (job_name, source_type, source_location, target_type, target_location, column_mapping, key_mapping, recon_type, sql_query, email_to, email_cc, notify_on_success, notify_on_failure)
VALUES ('s2t_customer_recon', 'csv', '/path/to/source/customer.csv', 'csv', '/path/to/target/customer.csv', 'id:cust_id,name:cust_name,email:cust_email', 'id', 'S2T', NULL, 'user@example.com', NULL, 'Y', 'Y');
```

**Business Rule (SQL) Reconciliation**

```sql
INSERT INTO recon_config (job_name, source_type, source_location, target_type, target_location, column_mapping, key_mapping, recon_type, sql_query, email_to, email_cc, notify_on_success, notify_on_failure)
VALUES ('business_rule_order_check', 'hive', 'sales.orders', NULL, NULL, NULL, NULL, 'SQL', 'SELECT * FROM source_view WHERE order_status = ''PENDING'' AND order_date < (SYSDATE - 7)', 'user@example.com', 'manager@example.com', 'N', 'Y');
```
