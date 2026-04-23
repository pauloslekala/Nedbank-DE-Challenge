"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
#from delta import configure_spark_with_delta_pip
from utils.spark import get_spark_session
import yaml, os


config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
with open(config_path) as f:
    config = yaml.safe_load(f)


print(config['input']['accounts_path'])
spark = get_spark_session()


def run_ingestion():
    # TODO: Implement Bronze layer ingestion.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise a SparkSession with Delta Lake support (local[2]).
    #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
    #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
    #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.
    

    #loading accounts data
    df_acc = spark.read.format("csv")\
                  .options(header= True)\
                  .load(config['input']['accounts_path'])
    
    df_acc = df_acc.withColumn("ingestion_timestamp",current_timestamp())\
                    .withColumn("source",lit("accounts"))
    
    df_acc.write.format("delta")\
          .mode("overwrite")\
          .partitionBy("source")\
          .save(f"{config['output']['bronze_path']}/accounts")
    


    #loading customer data
    df_cust = spark.read.format("csv")\
                  .options(header= True)\
                  .load(config['input']['customers_path'])
    
    df_cust = df_cust.withColumn("ingestion_timestamp",current_timestamp())\
                      .withColumn("source",lit("customers"))
    
    df_cust.write.format("delta")\
          .mode("overwrite")\
          .partitionBy("source")\
          .save(f"{config['output']['bronze_path']}/customers")
    
  


      #loading transaction data
    df_trans = spark.read.format("json")\
                  .options(header= True)\
                  .load(config['input']['transactions_path'])
    
    df_trans = df_trans.withColumn("ingestion_timestamp",current_timestamp())\
                        .withColumn("source",lit("transactions"))
    
    print(df_trans.printSchema)
    
    df_trans.write.format("delta")\
          .mode("overwrite")\
          .partitionBy("source")\
          .save(f"{config['output']['bronze_path']}/transactions")

    
