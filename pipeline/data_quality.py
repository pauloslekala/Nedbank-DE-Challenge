"""
Data Quality detection, flagging, and report generation.

"""

import json
import os
import time
import yaml
from datetime import datetime, timezone
from pyspark import StorageLevel

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
col, lit, when, to_date, unix_timestamp,
regexp_replace, trim, upper, count, sum as spark_sum,
row_number, coalesce, broadcast
)
from pyspark.sql.types import DecimalType, DateType
from pyspark.sql.window import Window


# Load DQ rules

def load_dq_rules() -> dict:
    rules_path =  "/data/config/dq_rules.yaml"
    with open(rules_path) as f:
        full = yaml.safe_load(f)
    return full


# Date normalisation 
# Handles: YYYY-MM-DD, DD/MM/YYYY, Unix epoch integer (as string or long)

def normalise_date_col(df: DataFrame, col_name: str) -> DataFrame:
    """
    Returns df with col_name cast to DateType, handling three formats.
    Adds a boolean column <col_name>_was_noniso = True when format was not YYYY-MM-DD.
    """
    raw = col(col_name).cast("string")

    # Unix epoch: all digits, 8-12 chars
    is_epoch = raw.rlike(r"^\d{8,12}$")

    # DD/MM/YYYY:
    is_dmy = raw.rlike(r"^\d{2}/\d{2}/\d{4}$")

    normalised = (
    when(is_epoch,
    to_date(
    (col(col_name).cast("long")).cast("timestamp")
    ))
    .when(is_dmy,
    to_date(raw, "dd/MM/yyyy"))
    .otherwise(
    to_date(raw, "yyyy-MM-dd"))
    )

    was_noniso = (is_epoch | is_dmy)

    return (
    df
    .withColumn(col_name, normalised)
    .withColumn(f"{col_name}_was_noniso", was_noniso)
    )

def apply_date_format_checks(df: DataFrame, rules: dict) -> DataFrame:
    """
    Applies date normalisation to all columns listed under
    date_format_checks in dq_rules.yaml.
    Marks the row as DATE_FORMAT if any of them were non-ISO.
    """
    date_cols = rules.get("date_format_checks", [])

    for col_name in date_cols:
        if col_name in df.columns:
            df = normalise_date_col(df, col_name)

    # Combine all _was_noniso flags into one
    noniso_flags = [
    f"{c}_was_noniso" for c in date_cols
    if f"{c}_was_noniso" in df.columns
    ]

    if noniso_flags:
        combined = lit(False)
        for flag in noniso_flags:
            combined = combined | col(flag)
        df = df.withColumn("any_date_was_noniso", combined)
        # Drop individual flags — only keep the combined one
        df = df.drop(*noniso_flags)
    else:
        df = df.withColumn("any_date_was_noniso", lit(False))

    return df

# Amount type detection and cast 

def detect_and_cast_amount(df: DataFrame) -> DataFrame:
    """
    Detects amount delivered as STRING (quoted number).
    Casts amount to DECIMAL(18,2).
    Adds:
    amount_was_string — boolean, True when original type was string
    amount_cast_failed — boolean, True when cast to DECIMAL failed
    
    """

    amount_dtype = dict(df.dtypes).get("amount", "")

    if "string" in amount_dtype.lower():
# Field arrived as string — attempt cast
        cleaned = regexp_replace(trim(col("amount")), r"[^0-9.\-]", "")
        casted = cleaned.cast(DecimalType(18, 2))
        df = (
        df
        .withColumn("amount_was_string", lit(True))
        .withColumn("amount_cast_failed", casted.isNull() & col("amount").isNotNull())
        .withColumn("amount", casted)
        )
    else:
        df = (
        df
        .withColumn("amount_was_string", lit(False))
        .withColumn("amount_cast_failed", lit(False))
        .withColumn("amount", col("amount").cast(DecimalType(18, 2)))
        )

    return df


# Currency normalisation 

def normalise_currency(df: DataFrame, rules: dict) -> DataFrame:
    """
    Standardises currency variants to ZAR.
    Marks records where currency was not already ZAR.
    Variants: R, rands, 710, zar (and any case variant).
    """

    currency_rules = rules.get("currency_normalisation", {})
    target         = currency_rules.get("target_value", "ZAR")

    upper_curr = upper(trim(col("currency").cast("string")))
    is_variant = ~upper_curr.isin("ZAR") & col("currency").isNotNull()

    return (
    df
    .withColumn("currency_was_variant", is_variant)
    .withColumn("currency", lit(target))
    )


#  Duplicate detection 

def flag_duplicates_groupby(df: DataFrame, rules: dict) -> DataFrame:
    """
    Memory-efficient deduplication using groupBy + join instead of Window.
    Avoids full sort shuffle on large datasets.
    Keeps the row with the earliest ingestion_timestamp per transaction_id.
    """
    from pyspark.sql.functions import min as spark_min

    dedup_config = rules.get("deduplication", {}).get("fact_transactions", {})
    key          = dedup_config.get("key", "transaction_id")
    order_col    = dedup_config.get("order_by", "ingestion_timestamp")

    # Find the earliest timestamp per transaction_id — cheap aggregation
    df_min = (
        df.groupBy(key)
        .agg(spark_min(order_col).alias(f"_min_{order_col}"))
    )

    # Join back — only rows matching the earliest timestamp survive
    df_deduped = (
        df.join(df_min, on=key, how="inner")
        .withColumn(
            "is_duplicate",
            col(order_col) != col(f"_min_{order_col}")
        )
        .drop(f"_min_{order_col}")
    )

    return df_deduped

def flag_duplicates(df: DataFrame, rules: dict) -> DataFrame:
    """
    Keeps the record with the earliest ingestion_timestamp per transaction_id.
    Marks duplicates with is_duplicate = True.
    """

    dedup_config = rules.get("deduplication", {}).get("fact_transactions", {})
    key = dedup_config.get("key", "transaction_id")
    order_col = dedup_config.get("order_by", "ingestion_timestamp")
    order_dir = dedup_config.get("order","")

    order_expr = col(order_col).asc() if order_dir == "asc" else col(order_col).desc()

    w = Window.partitionBy(key).orderBy(order_expr)
    return (
    df
    .withColumn("_rn", row_number().over(w))
    .withColumn("is_duplicate", col("_rn") > 1)
    .drop("_rn")
    )


# Orphan detection 

def flag_orphans(df_trans: DataFrame, df_accounts: DataFrame) -> DataFrame:
    """
    Marks transactions whose account_id has no match in accounts Silver table.
    """
    valid_accounts = df_accounts.select("account_id").distinct()
    return (
    df_trans
    .join(valid_accounts, on="account_id", how="left_anti")
    .withColumn("is_orphan", lit(True))
    .union(
    df_trans
    .join(valid_accounts, on="account_id", how="inner")
    .withColumn("is_orphan", lit(False))
    )
    )

def flag_domain_violations(df: DataFrame, rules: dict) -> DataFrame:
    """
    Flags records where transaction_type or channel contain values
    outside the allowed domain defined in dq_rules.yaml.
    """
    domain_checks = rules.get("domain_checks", {})

    # Build violation condition from rules
    violation = lit(False)

    for field, rule in domain_checks.items():
        allowed = rule.get("allowed", [])
    if allowed and field in df.columns:
        field_violation = ~col(field).isin(allowed) & col(field).isNotNull()
        violation = violation | field_violation

    return df.withColumn("is_domain_violation", violation)

def apply_null_checks(df: DataFrame, rules: dict, table: str = "fact_transactions") -> DataFrame:
    """
    Flags records with null values in required fields defined under
    null_checks in dq_rules.yaml.
    """
    required_fields = rules.get("null_checks", {}).get(table, [])

    if not required_fields:
        return df.withColumn("has_null_required", lit(False))

    # Build null condition across all required fields
    null_condition = lit(False)
    for field in required_fields:
        if field in df.columns:
            null_condition = null_condition | col(field).isNull()

    return df.withColumn("has_null_required", null_condition)

# Master DQ function

def apply_transaction_dq(
df_trans: DataFrame,
df_accounts_silver: DataFrame,
rules: dict,
) -> DataFrame:
    """
    Applies all DQ checks to the Bronze transactions DataFrame.
    Returns a DataFrame with:
    - dq_flag column (NULL for clean, issue code for flagged)
    - Helper boolean columns for reporting (dropped before Gold write)
    """

    # 1. Amount type mismatch
    df = detect_and_cast_amount(df_trans)

    # 2. Currency variants
    df = normalise_currency(df, rules)

    # 3. Date format normalisation
    df = apply_date_format_checks(df, rules)

    df = apply_null_checks(df, rules) 

    # 4. Duplicate detection
    df = flag_duplicates(df, rules)

    df = flag_domain_violations(df, rules)

    # 5. Orphan detection (against Silver accounts — already deduplicated)
    valid_accs = df_accounts_silver.select("account_id").distinct()
    valid_accs = broadcast(valid_accs)

    df = (
    df
    .join(valid_accs.withColumnRenamed("account_id", "_valid_acc"),
    col("account_id") == col("_valid_acc"), how="left")
    .withColumn("is_orphan", col("_valid_acc").isNull())
    .drop("_valid_acc")
    )

    # 6. Build dq_flag — priority order matters
    # ORPHANED_ACCOUNT > DUPLICATE_DEDUPED > TYPE_MISMATCH > DATE_FORMAT > CURRENCY_VARIANT

    ih = rules.get("issue_handling", {})
    orphan_flag = ih["orphaned_transactions"]["dq_flag"]
    duplicate_flag = ih["duplicate_transactions"]["dq_flag"]
    type_flag = ih["amount_type_mismatch"]["dq_flag"]
    date_flag = ih["date_format_inconsistency"]["dq_flag"]
    currency_flag = ih["currency_variants"]["dq_flag"]
    null_flag      = ih["null_account_id"]["dq_flag"]

    df = df.withColumn(
    "dq_flag",
    when(col("is_orphan"), lit(orphan_flag))
    .when(col("is_duplicate"), lit(duplicate_flag))
    .when(col("amount_cast_failed"), lit(type_flag))
    #.when(col("is_domain_violation"), lit(type_flag)) 
    .when(col("has_null_required"), lit(null_flag))
    .when(col("any_date_was_noniso"), lit(date_flag))
    .when(col("currency_was_variant"), lit(currency_flag))
    .otherwise(lit(None).cast("string"))
    )

    return df


def apply_account_dq(df_accounts: DataFrame, rules: dict) -> DataFrame:
    """
    Flags null account_id records.
    Returns (clean_df, null_pk_count).
    """

    pk_config = rules.get("null_primary_key", {}).get("dim_accounts", {})
    pk_field  = pk_config.get("field", "account_id")

    null_pk_count = df_accounts.filter(col(pk_field).isNull()).count()
    clean = df_accounts.filter(col(pk_field).isNotNull())
    return clean, null_pk_count


# Report generation 

def generate_dq_report(
df_trans_flagged: DataFrame,
df_accounts_raw: DataFrame,
df_customers_raw: DataFrame,
null_pk_count: int,
rules: dict,
config: dict,
pipeline_start_time: float,
) -> None:
    """
    Computes counts for each DQ issue category and writes dq_report.json.
    """
    
    df_trans_flagged = df_trans_flagged.cache()

    count_row = df_trans_flagged.agg(
        spark_sum(lit(1)).alias("total"),
        spark_sum(when(col("dq_flag") == "DUPLICATE_DEDUPED", 1).otherwise(0)).alias("duplicates"),
        spark_sum(when(col("dq_flag") == "ORPHANED_ACCOUNT",  1).otherwise(0)).alias("orphans"),
        spark_sum(when(col("dq_flag") == "TYPE_MISMATCH",     1).otherwise(0)).alias("type_mismatch"),
        spark_sum(when(col("dq_flag") == "DATE_FORMAT",       1).otherwise(0)).alias("date_format"),
        spark_sum(when(col("dq_flag") == "CURRENCY_VARIANT",  1).otherwise(0)).alias("currency"),
        spark_sum(when(col("dq_flag").isNull() |
                      ~col("dq_flag").isin(["ORPHANED_ACCOUNT","DUPLICATE_DEDUPED"]), 1)
                 .otherwise(0)).alias("gold_count"),
    ).collect()[0]

    trans_raw_count     = count_row["total"]
    duplicate_count     = count_row["duplicates"]
    orphan_count        = count_row["orphans"]
    type_mismatch_count = count_row["type_mismatch"]
    date_format_count   = count_row["date_format"]
    currency_count      = count_row["currency"]
    gold_trans_count    = count_row["gold_count"]

    # These are small tables — counts are cheap
    accounts_raw_count  = df_accounts_raw.count()
    customers_raw_count = df_customers_raw.count()
    gold_accounts_count  = df_accounts_raw.filter(col("account_id").isNotNull()).count()
    gold_customers_count = customers_raw_count

    # flag_map for flag_counts field
    ih = rules.get("issue_handling", {})
    flag_map = {
        ih["duplicate_transactions"]["dq_flag"]:   duplicate_count,
        ih["orphaned_transactions"]["dq_flag"]:    orphan_count,
        ih["amount_type_mismatch"]["dq_flag"]:     type_mismatch_count,
        ih["date_format_inconsistency"]["dq_flag"]: date_format_count,
        ih["currency_variants"]["dq_flag"]:        currency_count,
    }
    # Remove zero-count entries
    flag_map = {k: v for k, v in flag_map.items() if v > 0}

    def pct(n, total):
        return round(n * 100.0 / total, 2) if total > 0 else 0.0


    """
    df_trans_flagged = df_trans_flagged.persist(StorageLevel.DISK_ONLY)
    trans_raw_count = df_trans_flagged.count()
    accounts_raw_count = df_accounts_raw.count()
    customers_raw_count = df_customers_raw.count()

    # Count per dq_flag value
    flag_counts_df = (
    df_trans_flagged
    #.repartition(4)
    .groupBy("dq_flag")
    .count()
    .collect()
    )
    flag_map = {row["dq_flag"]: row["count"] for row in flag_counts_df if row["dq_flag"]}

    # Helper: safe percentage
    def pct(n, total):
        return round(n * 100.0 / total, 2) if total > 0 else 0.0
    
    ih = rules.get("issue_handling", {})

    # Map flag counts to issue types 
    duplicate_count = flag_map.get(ih["duplicate_transactions"]["dq_flag"], 0)
    orphan_count = flag_map.get(ih["orphaned_transactions"]["dq_flag"], 0)
    type_mismatch_count = flag_map.get(ih["amount_type_mismatch"]["dq_flag"], 0)
    date_format_count = flag_map.get(ih["date_format_inconsistency"]["dq_flag"], 0)
    currency_count = flag_map.get(ih["currency_variants"]["dq_flag"], 0)

    # Gold counts — records that make it through (non-orphan, non-duplicate)
    excluded_flags = {
        ih["orphaned_transactions"]["dq_flag"],
        ih["duplicate_transactions"]["dq_flag"]
    }
    gold_trans_count = (
    df_trans_flagged
    .filter(~col("dq_flag").isin(list(excluded_flags)) | col("dq_flag").isNull())
    .count()
    )
    
    df_trans_flagged.unpersist()

    gold_accounts_count = df_accounts_raw.filter(col("account_id").isNotNull()).count()
    gold_customers_count = customers_raw_count
    """

    dq_issues = []

    if duplicate_count > 0:
        dq_issues.append({
        "issue_type": ih["duplicate_transactions"]["issue_type"],
        "records_affected": duplicate_count,
        "percentage_of_total": pct(duplicate_count, trans_raw_count),
        "handling_action": ih["duplicate_transactions"]["handling_action"],
        "records_in_output": 0,
        })

    if orphan_count > 0:
        dq_issues.append({
        "issue_type": ih["orphaned_transactions"]["issue_type"],
        "records_affected": orphan_count,
        "percentage_of_total": pct(orphan_count, trans_raw_count),
        "handling_action": ih["orphaned_transactions"]["handling_action"],
        "records_in_output": 0,
        })

    if type_mismatch_count > 0:
        dq_issues.append({
        "issue_type": ih["amount_type_mismatch"]["issue_type"],
        "records_affected": type_mismatch_count,
        "percentage_of_total": pct(type_mismatch_count, trans_raw_count),
        "handling_action": ih["amount_type_mismatch"]["handling_action"],
        "records_in_output": type_mismatch_count,
        })

    if date_format_count > 0:
        dq_issues.append({
        "issue_type": ih["date_format_inconsistency"]["issue_type"],
        "records_affected": date_format_count,
        "percentage_of_total": pct(date_format_count, trans_raw_count),
        "handling_action": ih["date_format_inconsistency"]["handling_action"],
        "records_in_output": date_format_count,
        })

    if currency_count > 0:
        dq_issues.append({
        "issue_type": ih["currency_variants"]["issue_type"],
        "records_affected": currency_count,
        "percentage_of_total": pct(currency_count, trans_raw_count),
        "handling_action": ih["currency_variants"]["handling_action"],
        "records_in_output": currency_count,
        })

    if null_pk_count > 0:
        dq_issues.append({
        "issue_type": ih["null_account_id"]["issue_type"],
        "records_affected": null_pk_count,
        "percentage_of_total": pct(null_pk_count, accounts_raw_count),
        "handling_action": ih["null_account_id"]["handling_action"],
        "records_in_output": 0,
        })

    report = {
    "$schema": "nedbank-de-challenge/dq-report/v1",
    "run_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "stage": "2",
    "source_record_counts": {
    "accounts_raw": accounts_raw_count,
    "transactions_raw": trans_raw_count,
    "customers_raw": customers_raw_count,
    },
    "dq_issues": dq_issues,
    "gold_layer_record_counts": {
    "fact_transactions": gold_trans_count,
    "dim_accounts": gold_accounts_count,
    "dim_customers": gold_customers_count,
    },
    "execution_duration_seconds": int(time.time() - pipeline_start_time),
    # Required by Check 6 scorer validation
    "total_records": trans_raw_count,
    "clean_records": gold_trans_count,
    "flagged_records": trans_raw_count - gold_trans_count,
    "flag_counts": {k: v for k, v in flag_map.items()},
    }

    output_path = "/data/output/dq_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[DQ] Report written to {output_path}")
    print(f"[DQ] Issues found: {[i['issue_type'] for i in dq_issues]}")
    print(f"[DQ] Flag counts: {flag_map}")