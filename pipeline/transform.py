"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from utils.spark import get_spark_session
from utils.schema import *
import yaml, os


spark = get_spark_session()


def run_transformation():
    # TODO: Implement Silver layer transformation.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read each Bronze table.
    #   4. Deduplicate, type-cast, and standardise each table.
    #   5. Apply DQ flagging to the transactions table.
    #   6. Write cleaned tables to silver/.


  config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
  with open(config_path) as f:
    config = yaml.safe_load(f)


# customers transformation

  df_cust_brz = spark.read \
                    .format('delta')\
                    .option("header",True)\
                    .load(f"{config['output']['bronze_path']}/customers")

  
  
  df_cust_brz = schema_enforce(df_cust_brz,cust_schema)

  window = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
  df_cust_brz = df_cust_brz.withColumn("row_num",row_number().over(window))\
                          .filter("row_num=1").drop("row_num")

  df_cust_brz = df_cust_brz.filter(col("customer_id").isNotNull()) 


  df_cust_brz.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['silver_path']}/customers")


# accounts transformation

  df_acc_brz = spark.read \
                      .format('delta')\
                      .option("header",True)\
                      .load(f"{config['output']['bronze_path']}/accounts")
    

  df_acc_brz = schema_enforce(df_acc_brz,acc_schema)

  window = Window.partitionBy("account_id").orderBy(col("ingestion_timestamp").desc())
  df_acc_brz = df_acc_brz.withColumn("row_num",row_number().over(window))\
                          .filter("row_num=1").drop("row_num")
  
  df_acc_brz = df_acc_brz.join(df_cust_brz.select('customer_id')\
                                 ,col("customer_id") == col("customer_ref")\
                                 ,how='inner').drop("customer_ref")
   
  df_acc_brz = df_acc_brz.filter(col("account_id").isNotNull())
 
  
  df_acc_brz.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['silver_path']}/accounts")




# transaction transformation

  df_trans_brz = spark.read \
                    .format('delta')\
                    .option("header",True)\
                    .load(f"{config['output']['bronze_path']}/transactions")
   
  df_trans_brz = df_trans_brz.withColumn("province", col("location.province")) \
                              .withColumn("city", col("location.city")) \
                              .withColumn("coordinates", col("location.coordinates")) \
                              .withColumn("device_id", col("metadata.device_id")) \
                              .withColumn("session_id", col("metadata.session_id")) \
                              .withColumn("retry_flag", col("metadata.retry_flag")) \
                              .drop("location", "metadata")
  
  if "merchant_subcategory" not in df_trans_brz.columns:
    #df_trans_brz = df_trans_brz.withColumn("merchant_subcategory", lit(None).cast("string"))
    df_trans_brz = df_trans_brz.select("*", lit(None).cast("string").alias("merchant_subcategory"))
                                                                           

  
  df_trans_brz = schema_enforce(df_trans_brz,trans_schema)

  window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
  df_trans_brz = df_trans_brz.withColumn("row_num",row_number().over(window))\
                          .filter("row_num=1").drop("row_num")
  
  df_trans_brz = df_trans_brz.withColumn("currency",lit("ZAR").cast("string"))
 
  
  df_trans_brz = df_trans_brz.join(df_acc_brz.select('account_id')\
                                 ,on="account_id"\
                                 ,how='inner')
  
  df_trans_brz = df_trans_brz.filter(col('transaction_id').isNotNull())


  df_trans_brz.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['silver_path']}/transactions")
  
