import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import warnings
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType, TimestampType, DoubleType, LongType
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from datetime import datetime
import random


def item_code(item):
    code = ""
    for idx, val in enumerate(item):
        if idx == 0 or idx == len(item)-1:
            code+=str(ord(val))
        else:
            code+=val
    return code

spark = SparkSession.builder.appName("prepare").getOrCreate()

date_str_formula = datetime.now().strftime("%Y%m%d")
orders = spark.read.option("header", "true").parquet(f"gs://store_data_lake_online-store-382421/raw/orders{date_str_formula}.parquet.snappy")
customers = spark.read.option("header", "true").parquet(f"gs://store_data_lake_online-store-382421/raw/customers{date_str_formula}.parquet.snappy")
items = spark.read.option("header", "true").parquet(f"gs://store_data_lake_online-store-382421/raw/items{date_str_formula}.parquet.snappy")

pre_items = items.withColumn("item", items['item'].cast(StringType())) \
                 .withColumn("price", items['price'].cast(IntegerType())) \
                 .withColumn("description", items['description'].cast(StringType())) \
                 .withColumn("discount_flag", items['discount_flag'].cast(StringType())) \
                 .withColumn("last_day_score", items['last_day_score'].cast(DoubleType()))
pre_orders = orders.withColumn("customer_id", orders['customer_id'].cast(IntegerType())) \
                   .withColumn("item", orders['item'].cast(StringType())) \
                   .withColumn("created_at", orders['created_at'].cast(DateType()))
pre_customers = customers.withColumn("id", customers['id'].cast(IntegerType())) \
                         .withColumn("Date of Birth", customers['Date of Birth'].cast(DateType())) \
                         .withColumn("country", customers['country'].cast(StringType())) \
                         .withColumn("address", customers['address'].cast(StringType())) \
                         .withColumn("gender", customers['gender'].cast(StringType())) \
                         .withColumn("credit_card_provider", customers['credit_card_provider'].cast(StringType())) \
                         .withColumn("credit_card_number", customers['credit_card_number'].cast(StringType())) \
                         .withColumn("credit_card_expire", customers['credit_card_expire'].cast(StringType())) \
                         .withColumn("credit_card_security_code", customers['credit_card_security_code'].cast(IntegerType())) \
                         .withColumnRenamed("Date of Birth", "date_of_birth")

item_codes_maker = F.udf(item_code, returnType = StringType())
pre_items = items.withColumn("iid", item_codes_maker(items["item"]))

pre_orders = pre_orders.join(pre_items, pre_orders["item"] == pre_items["item"], "inner") \
                       .select(["customer_id", "iid", "created_at"])

pre_orders.repartition(4).write.option("compression", "gzip").csv("gs://store_data_lake_online-store-382421/processed/orders.csv.gz")
pre_customers.write.option("compression", "gzip").csv("gs://store_data_lake_online-store-382421/processed/customers.csv.gz")
pre_items.write.option("compression", "gzip").csv("gs://store_data_lake_online-store-382421/processed/items.csv.gz")
                         