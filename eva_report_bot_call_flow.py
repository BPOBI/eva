# Databricks notebook source
import requests
from tenacity import Retrying, RetryError, wait_random_exponential, stop_after_attempt
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, col, lit,when
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC #Bot Call Flow

# COMMAND ----------

eva_tags = spark.table("eva.eva_api_tags")

# COMMAND ----------

# This function converts the string cell into a date:
from pyspark.sql.types import DateType
tag_data = eva_tags.withColumn("record_date",eva_tags['conversationStartedAt'].cast(DateType()))

# COMMAND ----------

tag_data = tag_data.filter(F.col('record_date')>='2024-02-06')

# COMMAND ----------

tag_data_duration = tag_data.dropDuplicates(["record_date", "sessionCode"])

# COMMAND ----------

tag_data_filter = tag_data.withColumn("By_Design_Flag", when(col("tag_name").isin('#genesys_call_drop','#cant_hear_other_person','#app_cant_open','#update_install_software'),1).otherwise(0))

# COMMAND ----------

tag_data_duration = tag_data_duration.withColumn("duration_minutes",F.round(F.col("durationInSeconds")/60,2))

# COMMAND ----------

tag_data_call_offered = tag_data_duration.groupBy("record_date") \
                                .agg(F.countDistinct("sessionCode").alias("call_offered"),
                                     F.sum("duration_minutes").alias("total_duration_minutes"))

# COMMAND ----------

 tag_data_call_offered = tag_data_call_offered.withColumn("AHT",F.round(F.col("total_duration_minutes")/F.col("call_offered"),2))

# COMMAND ----------

tag_data_filtered = tag_data.filter(F.col("tag_name").isin(["#connect_to_agent","#issue_resolved"]))

# COMMAND ----------

tag_data_filtered  = tag_data_filtered.dropDuplicates(["sessionCode","record_date","tag_name"])

# COMMAND ----------

grouping_tag = tag_data_filtered.groupBy("record_date") \
                       .agg(F.count(F.when(F.col("tag_name") == "#connect_to_agent", 1)).alias("IVR_Agent_Offered"), 
                            (F.count(F.when(F.col("tag_name")=="#connect_to_agent_requested",1)).alias('Caller_Requested')),
                            
                            F.count(F.when(F.col("tag_name") == "#issue_resolved", 1)).alias("IVR_Call_Contained")) \
                       .orderBy("record_date")

# COMMAND ----------

Bot_Call_Flow = tag_data_call_offered.join(grouping_tag, (tag_data_call_offered.record_date == grouping_tag.record_date),"left").select(tag_data_call_offered["*"],grouping_tag["IVR_Agent_Offered"], grouping_tag["IVR_Call_Contained"])

# COMMAND ----------

Bot_Call_Flow = Bot_Call_Flow.withColumn("IVR_Call_Abandoned", F.col("call_offered") - (F.col("IVR_Agent_Offered")+(F.col("IVR_Call_Contained"))))\
                            .withColumn('Abandoned%',F.round(F.col("IVR_Call_Abandoned")/F.col("call_offered")*100,2))\
                            .withColumn('Contained%',F.round(F.col('IVR_Call_Contained')/F.col("call_offered")*100,2)).orderBy("record_date")

# COMMAND ----------

Bot_Call_Flow = Bot_Call_Flow.withColumn("Abandoned%",(concat(("Abandoned%"),lit("%"))))\
                             .withColumn("Contained%",concat(("Contained%"),lit("%"))).select("record_date","call_offered","IVR_Call_Abandoned","Abandoned%","IVR_Agent_Offered","IVR_Call_Contained","Contained%","AHT").orderBy("record_date")

# COMMAND ----------

tag_data_filter  = tag_data_filter.dropDuplicates(["sessionCode","record_date","tag_name"])

# COMMAND ----------

coversation_flow = tag_data_filter.groupBy("record_date") \
                       .agg(F.count(F.when(F.col("By_Design_Flag") == 1, 1)).alias("By_Design"), 
                            (F.count(F.when(F.col("tag_name")=="#connect_to_agent_requested",1)).alias('Caller_Requested')))

# COMMAND ----------

Coversation_Flow_Report = tag_data_call_offered.join(coversation_flow, (tag_data_call_offered.record_date == coversation_flow.record_date),"left").select(tag_data_call_offered["*"],coversation_flow["Caller_Requested"], coversation_flow["By_Design"]).drop('Total_Duration(Mintues)','AHT')

# COMMAND ----------

Bot_Call_Flow = Bot_Call_Flow.join(Coversation_Flow_Report,
                                   ["record_date","call_offered"],
                                   "left").select(Bot_Call_Flow["*"],Coversation_Flow_Report["Caller_Requested"],Coversation_Flow_Report["By_Design"]).orderBy("record_date")

# COMMAND ----------

min_max = Bot_Call_Flow.select(F.date_format(F.min("record_date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("record_date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------

target_table = "eva.report_bot_call_flow"

# recreate = True
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")

Bot_Call_Flow.write.partitionBy("record_date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"record_date >= '{min_date}' and record_date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------


