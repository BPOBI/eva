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

display(user_interaction)

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

grouping = user_interaction_reporting.groupBy("record_date")\
                                         .agg(F.count(F.when(F.col("user_sent") == 1,1)).alias("Text_Request"),
                                             (F.sum("Duratin_mintues").alias("Total_Duration(Mintues)")),
                                             (F.sum("token_usage").alias("token_usage")),
                                             (F.countDistinct("session_code").alias("conversation_count"))).orderBy("record_date")

# COMMAND ----------

grouping_df = grouping.withColumn("Duration_Average",F.round(F.col("Total_Duration(Mintues)")/F.col("conversation_count"),2))\
                      .withColumn("Total_Duration(Mintues)", F.round(F.col("Total_Duration(Mintues)"),2))

# COMMAND ----------

Billing_report = grouping_df.withColumn("Text_Request_Average",F.round(F.col("Text_Request")/F.col("conversation_count"),2))\
                      .withColumn("Text_Request_Average", F.round(F.col("Text_Request_Average"),2))

# COMMAND ----------

display(Billing_report)

# COMMAND ----------


