# Databricks notebook source
import requests
from tenacity import Retrying, RetryError, wait_random_exponential, stop_after_attempt
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, col, lit,when
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

user_interaction = spark.table("eva.eva_user_interaction")

# COMMAND ----------

genai_token = spark.table("eva.eva_genai_requests_usage")

# COMMAND ----------

window_spec = Window.partitionBy("session_code")

# COMMAND ----------

user_interaction = user_interaction.withColumn("start_date",min("create_date").over(window_spec))\
                                   .withColumn("end_date",max("create_date").over(window_spec))\
                                 

# COMMAND ----------

user_interaction = user_interaction.withColumn("durationInSeconds", (F.col("end_date").cast("long") - F.col("start_date").cast("long")))

# COMMAND ----------

user_interaction = user_interaction.withColumn("Duratin_mintues",F.round(F.col("durationInSeconds")/60,2))

# COMMAND ----------

# This function converts the string cell into a date:
from pyspark.sql.types import DateType
user_interaction = user_interaction.withColumn("record_date",user_interaction['create_date'].cast(DateType()))

# COMMAND ----------

genai_token_grouping = genai_token.groupBy("session_code")\
    .agg(F.round(F.avg("used_tokens")).alias("token_usage"))

# COMMAND ----------

user_interaction_reporting = user_interaction.join(genai_token_grouping, (user_interaction.session_code == genai_token_grouping.session_code),"left").select(user_interaction["*"],genai_token_grouping["token_usage"])

# COMMAND ----------

user_interaction_reporting_user= user_interaction_reporting.filter(F.col("user_sent")==1)

# COMMAND ----------

user_text = user_interaction_reporting_user.groupBy("record_date","session_code","text")\
                                         .agg(F.sum((F.col("user_sent"))).alias("text_request"))

# COMMAND ----------

user_text = user_text.groupBy("record_date")\
                                         .agg(F.sum(F.col("text_request")).alias("text_request"))

# COMMAND ----------

user_interaction_reporting = user_interaction_reporting.dropDuplicates(["record_date", "session_code"])

# COMMAND ----------

import pyspark.sql.functions as F

grouping = user_interaction_reporting.groupBy("record_date") \
    .agg(F.sum("duratin_mintues").alias("total_duration_mintues"),
        F.round(F.sum("token_usage")).alias("token_usage"),
         F.countDistinct("session_code").alias("conversation_count")) \
    .orderBy("record_date")

# COMMAND ----------

grouping = grouping.join(user_text,
                                             ['record_date'],
                                                             "left"
                                                             ).select(grouping["*"], user_text["Text_Request"])

# COMMAND ----------

grouping_df = grouping.withColumn("duration_average",F.round(F.col("total_duration_mintues")/F.col("conversation_count"),2))\
                      .withColumn("total_duration_mintues", F.round(F.col("total_duration_mintues"),2))

# COMMAND ----------

Billing_report = grouping_df.withColumn("text_request_average",F.round(F.col("text_request")/F.col("conversation_count"),2))\
                      .withColumn("text_request_average", F.round(F.col("text_request_average"),2)).orderBy("record_date")

# COMMAND ----------

min_max = Billing_report.select(F.date_format(F.min("record_date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("record_date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------

# recreate = False

target_table = "eva.report_billing"
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")

Billing_report.write.partitionBy("record_date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"record_date >= '{min_date}' and record_date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)
