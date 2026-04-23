"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from utils.spark import get_spark_session
from utils.schema import *
import yaml, os


spark = get_spark_session()

def run_provisioning():
    # TODO: Implement Gold layer provisioning.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read Silver tables.
    #   4. Build dim_customers with surrogate keys and derived age_band.
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    #   7. Write all three Gold tables as Delta Parquet.
    #   8. (Stage 2+) Write dq_report.json to /data/output/.


  config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
  with open(config_path) as f:
    config = yaml.safe_load(f)
    

  # customers provision
  df_cust_slv = spark.read \
                    .format('delta')\
                    .option("header",True)\
                    .load(f"{config['output']['silver_path']}/customers")
  
  df_cust_slv = df_cust_slv.withColumn("customer_sk",xxhash64(col("customer_id")))
  df_cust_slv = df_cust_slv.withColumn("age",
                                       floor(months_between(current_date(),col("dob"))/12)
                                       )
  df_cust_slv = df_cust_slv.withColumn("age_band",
                                       when(col("age") >= 65, "65+")
                                      .when(col("age") >= 56, "56-65")
                                      .when(col("age") >= 46, "46-55")
                                      .when(col("age") >= 36, "36-45")
                                      .when(col("age") >= 26, "26-35")
                                      .when(col("age") >= 18, "18-25")
                                      .otherwise(None)
                                      )
  

  df_cust_slv = df_cust_slv.drop("dob","age")


  # accounts provision

  df_acc_slv = spark.read \
                      .format('delta')\
                      .option("header",True)\
                      .load(f"{config['output']['silver_path']}/accounts")
  
  df_acc_slv = df_acc_slv.withColumn("account_sk",xxhash64(col("account_id")))
  df_acc_slv = df_acc_slv.withColumnRenamed("customer_ref","customer_id")
  df_acc_slv = df_acc_slv.join(df_cust_slv.select("customer_id","customer_sk")\
                               ,on= "customer_id"\
                               ,how="inner"
                               )
  

  # transaction provision

  df_trans_slv = spark.read \
                    .format('delta')\
                    .option("header",True)\
                    .load(f"{config['output']['silver_path']}/transactions")
  
  df_trans_slv = df_trans_slv.withColumn("transaction_sk",xxhash64(col("transaction_id")))
                              
  df_trans_slv.printSchema()
  df_trans_slv = df_trans_slv.join(df_acc_slv.select("account_id","customer_sk","account_sk")\
                               ,on= "account_id"\
                               ,how="inner"
                               )
  
  
  df_trans_slv = df_trans_slv.withColumn("transaction_timestamp",
                                         to_timestamp( 
                                        concat_ws(" ",col("transaction_date"),col("transaction_time")),
                                        "yyyy-MM-dd HH:mm:ss")
                                          ) 

 


  fact_transactions = df_trans_slv.select("transaction_sk","transaction_id","account_sk" \
                                          ,"customer_sk","transaction_date","transaction_timestamp" \
                                          ,"transaction_type","merchant_category","merchant_subcategory" \
                                          ,"amount","currency","channel","province"\
                                          ,"ingestion_timestamp"
                                          )
                                    
  # fact transactions
  
  fact_transactions.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['gold_path']}/fact_transactions")
  
  
  

  # dim customers

  dim_customers = df_cust_slv.select("customer_sk","customer_id","gender","province","income_band","segment" \
                                    ,"risk_score","kyc_status","age_band")
  
  dim_customers.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['gold_path']}/dim_customers")
  

  # dim accounts


  dim_accounts = df_acc_slv.select("account_sk","account_id","customer_id","account_type","account_status" \
                                   ,"open_date","product_tier","digital_channel","credit_limit" \
                                   ,"current_balance","last_activity_date")
  
  dim_accounts.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['gold_path']}/dim_accounts")
  

  
  