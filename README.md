# Data Reconciliation Framework

This is a config-driven data reconciliation framework built using Maven, Scala, and Spark.

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
  target/data-reconciliation-framework-1.0.0.jar <job-name>
```

Replace `<your-spark-master>` with your Spark master URL and `<job-name>` with the name of the job you want to run (as defined in the `recon_config` table).
