# Databricks notebook source
user = dbutils.secrets.get(scope = "bpo-keyvault", key = "evausername")
key = dbutils.secrets.get(scope = "bpo-keyvault", key = "evapassword")

# COMMAND ----------

jdbchostname  = "35.237.254.241"
jdbcport = 3306
jdbcproperties = {"user": user, "password": key}

tablename = 'Answer'

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

user_interaction.write.mode("overwrite").saveAsTable("eva.eva_user_interaction")

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

genai_requests_usage.write.mode("overwrite").saveAsTable("eva.eva_genai_requests_usage")

# COMMAND ----------


