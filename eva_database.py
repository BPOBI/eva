# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, col, lit,when

# COMMAND ----------

user = dbutils.secrets.get(scope = "bpo-keyvault", key = "evausername")
key = dbutils.secrets.get(scope = "bpo-keyvault", key = "evapassword")

# COMMAND ----------

jdbchostname  = "35.237.254.241"
jdbcport = 3306
jdbcproperties = {"user": user, "password": key}

# COMMAND ----------

db_fqdn = f"jdbc:mysql://{jdbchostname}:{jdbcport}"

# COMMAND ----------

# MAGIC %md
# MAGIC #Table1 - user interaction

# COMMAND ----------

user_interaction = (spark.read.format("jdbc")
       .option("url", db_fqdn)
       .option("user", user)
       .option("password", key)
       .option("query", "select * from production.user_interaction")
       .load())

# COMMAND ----------

with_conversation_df = user_interaction.withColumnRenamed("text","text_original")

# COMMAND ----------

with_conversation_df = with_conversation_df.withColumn("text", F.regexp_replace(col('text_original'), "<.*?>", ""))

# COMMAND ----------

with_conversation_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("eva.eva_user_interaction")

# COMMAND ----------

# MAGIC %md
# MAGIC #Table2 - genai_requests_usage

# COMMAND ----------

genai_requests_usage = (spark.read.format("jdbc")
       .option("url", db_fqdn)
       .option("user", user)
       .option("password", key)
       .option("query", "select * from production.genai_requests_usage")
       .load())

# COMMAND ----------

genai_requests_usage.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("eva.eva_genai_requests_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC #Table3 - tag

# COMMAND ----------

tag = (spark.read.format("jdbc")
       .option("url", db_fqdn)
       .option("user", user)
       .option("password", key)
       .option("query", "select * from production.tag")
       .load())

# COMMAND ----------

tag.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("eva.eva_tag")

# COMMAND ----------

# MAGIC %md
# MAGIC #Table4 - tag_use

# COMMAND ----------

tag_use = (spark.read.format("jdbc")
       .option("url", db_fqdn)
       .option("user", user)
       .option("password", key)
       .option("query", "select * from production.tag_use")
       .load())

# COMMAND ----------

tag_use.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("eva.eva_tag_use")

# COMMAND ----------


