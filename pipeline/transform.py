"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import StorageLevel
from utils.spark import get_spark_session
from utils.schema import *
import yaml, os, time

from data_quality import (
load_dq_rules,
apply_account_dq,
apply_date_format_checks,
generate_dq_report,
detect_and_cast_amount, 
normalise_currency,
apply_null_checks,
flag_duplicates_groupby
)

pipeline_start = time.time()

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

  rules = load_dq_rules()

# customers transformation

  df_cust_brz = spark.read \
                    .format('delta')\
                    .option("header",True)\
                    .load(f"{config['output']['bronze_path']}/customers")

  
  df_cust_brz = apply_date_format_checks(df_cust_brz, rules)
  df_cust_brz = df_cust_brz.drop("any_date_was_noniso")

  df_cust_brz = schema_enforce(df_cust_brz,cust_schema)

  window = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
  df_cust_brz = df_cust_brz.withColumn("row_num",row_number().over(window))\
                          .filter("row_num=1").drop("row_num")

  df_cust_brz = df_cust_brz.filter(col("customer_id").isNotNull()) 


  df_cust_brz.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['silver_path']}/customers")


# --------------------------accounts transformation---------------------------------

  df_acc_brz = spark.read \
                      .format('delta')\
                      .option("header",True)\
                      .load(f"{config['output']['bronze_path']}/accounts")
    
  df_acc_raw_for_report = df_acc_brz



  df_acc_brz = apply_date_format_checks(df_acc_brz, rules)
  df_acc_brz = df_acc_brz.drop("any_date_was_noniso")


  df_acc_brz, null_pk_count = apply_account_dq(df_acc_brz,rules)


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

  df_acc_brz = df_acc_brz.persist(StorageLevel.MEMORY_ONLY)


# ---------------------------transaction transformation---------------------------------

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
    
    df_trans_brz = df_trans_brz.select("*", lit(None).cast("string").alias("merchant_subcategory"))

  FLAT_PATH = "/data/output/silver/_trans_flat_tmp"

  df_trans_brz.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .save(FLAT_PATH)

  df_trans_flat = spark.read.format("delta").load(FLAT_PATH)

 

  df_col_checks = detect_and_cast_amount(df_trans_flat)
  df_col_checks = normalise_currency(df_col_checks, rules)
  df_col_checks = apply_date_format_checks(df_col_checks, rules)
  df_col_checks = apply_null_checks(df_col_checks, rules)

  COL_PATH = "/data/output/silver/_trans_col_tmp"
  df_col_checks.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .save(COL_PATH)

  import shutil
  shutil.rmtree(FLAT_PATH, ignore_errors=True)

  df_col_checked = spark.read.format("delta").load(COL_PATH).repartition(4, "transaction_id")

  

  df_deduped = flag_duplicates_groupby(df_col_checked, rules)

  DEDUP_PATH = "/data/output/silver/_trans_dedup_tmp"
  df_deduped.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .save(DEDUP_PATH)

  shutil.rmtree(COL_PATH, ignore_errors=True)

  df_deduped_disk = spark.read.format("delta").load(DEDUP_PATH)

  
  valid_accs = df_acc_brz.select("account_id").distinct()
  df_orphaned = (
      df_deduped_disk
      .join(
          valid_accs.withColumnRenamed("account_id", "_valid_acc"),
          col("account_id") == col("_valid_acc"),
          how="left"
      )
      .withColumn("is_orphan", col("_valid_acc").isNull())
      .drop("_valid_acc")
  )

  
  ih             = rules.get("issue_handling", {})
  orphan_flag    = ih["orphaned_transactions"]["dq_flag"]
  duplicate_flag = ih["duplicate_transactions"]["dq_flag"]
  type_flag      = ih["amount_type_mismatch"]["dq_flag"]
  date_flag      = ih["date_format_inconsistency"]["dq_flag"]
  currency_flag  = ih["currency_variants"]["dq_flag"]
  null_flag      = ih["null_account_id"]["dq_flag"]

  df_trans_flagged = df_orphaned.withColumn(
      "dq_flag",
      when(col("is_orphan"),            lit(orphan_flag))
      .when(col("is_duplicate"),        lit(duplicate_flag))
      .when(col("amount_cast_failed"),  lit(type_flag))
      .when(col("has_null_required"),   lit(null_flag))
      .when(col("any_date_was_noniso"), lit(date_flag))
      .when(col("currency_was_variant"),lit(currency_flag))
      .otherwise(lit(None).cast("string"))
  )

  
  FLAGGED_PATH = "/data/output/silver/_trans_flagged_tmp"
  df_trans_flagged.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .save(FLAGGED_PATH)

  shutil.rmtree(DEDUP_PATH, ignore_errors=True)

  df_trans_flagged_disk = spark.read.format("delta").load(FLAGGED_PATH)
 #------------------------------------------------------------------------
  generate_dq_report(
    df_trans_flagged=df_trans_flagged_disk,
    df_accounts_raw=df_acc_raw_for_report,
    df_customers_raw=df_cust_brz,
    null_pk_count=null_pk_count,
    rules=rules,
    config=config,
    pipeline_start_time=pipeline_start,
    )   
     


  ih = rules.get("issue_handling", {})
  excluded_flags = [
        ih["orphaned_transactions"]["dq_flag"],    # ORPHANED_ACCOUNT → QUARANTINED
        ih["duplicate_transactions"]["dq_flag"],   # DUPLICATE_DEDUPED → excluded
    ]
  retained_flags = [
        ih["amount_type_mismatch"]["dq_flag"],     # TYPE_MISMATCH → retained
        ih["date_format_inconsistency"]["dq_flag"],# DATE_FORMAT → retained
        ih["currency_variants"]["dq_flag"],        # CURRENCY_VARIANT → retained
        ih["null_account_id"]["dq_flag"],          # NULL_REQUIRED → retained
    ]
  
  df_trans_silver = df_trans_flagged.filter(
  col("dq_flag").isNull()
  | col("dq_flag").isin(retained_flags)
  )

  # Drop DQ helper columns before writing
  helper_cols = [
  "amount_was_string", "amount_cast_failed",
  "currency_was_variant", "transaction_date_was_noniso",
  "is_duplicate", "is_orphan",
  ]

  df_trans_silver = df_trans_silver.drop(*[c for c in helper_cols if c in df_trans_silver.columns])                                                                   

  
  df_trans_silver = schema_enforce(df_trans_brz,trans_schema)


  df_trans_silver.show(10)

  df_trans_silver.repartition(4).write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema",True)\
            .save(f"{config['output']['silver_path']}/transactions")
  
