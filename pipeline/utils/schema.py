
from pyspark.sql.types import *
from pyspark.sql.functions import *


def schema_enforce(df,schema):
      for field in schema.fields:
        df = df.withColumn(field.name,col(field.name).cast(field.dataType)
                             )
         
      return df

acc_schema = StructType([ 
        StructField('account_id', StringType(), False),
        StructField('customer_ref', StringType(), False),
        StructField('account_type', StringType(), False),
        StructField('account_status', StringType(), False),
        StructField('open_date', DateType(), False),
        StructField('product_tier', StringType(), False),
        StructField('mobile_number', StringType(), True),
        StructField('digital_channel', StringType(), False),
        StructField('credit_limit', DecimalType(18,2), True),
        StructField('current_balance',DecimalType(18,2), False),
        StructField('last_activity_date', DateType(), True),
        StructField('ingestion_timestamp', TimestampType(),False),
        StructField('source', StringType(),False)
     
    ])

cust_schema = StructType([ 
         StructField('customer_id', StringType(), False),
         StructField('id_number', StringType(), False),
         StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('dob', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('province', StringType(), True),
        StructField('income_band', StringType(), False),
        StructField('segment', StringType(), True),
        StructField('risk_score',StringType(), False),
        StructField('kyc_status', StringType(), False),
        StructField('product_flags', StringType(), False),
        StructField('ingestion_timestamp', TimestampType(),False),
        StructField('source', StringType(),False)
     
    ])


trans_schema = StructType([ 
         StructField('account_id', StringType(), False),
         StructField('amount', DecimalType(18,2), False),
         StructField('channel', StringType(), False),
        StructField('currency', StringType(), False),
        StructField('merchant_category', StringType(), True),
        StructField('merchant_subcategory', StringType(), True),
        StructField('transaction_date', StringType(), False),
        StructField('transaction_id', StringType(), False),
        StructField('transaction_time',StringType(), False),
        StructField('transaction_type', StringType(), False),
        StructField('province', StringType(), True),
        StructField('city', StringType(), True),
        StructField('coordinates', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('session_id', StringType(), True),
        StructField('retry_flag', BooleanType(), False),
        StructField('ingestion_timestamp', TimestampType(),False),
        StructField('source', StringType(),False)
     
    ])

