# Databricks notebook source
import requests
from tenacity import Retrying, RetryError, wait_random_exponential, stop_after_attempt
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, col, lit,when
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#generate the token and refresh 
auth_url = "https://keycloak-americas-admin.eva.bot/auth/realms/EVA-NTTLTD/protocol/openid-connect/token"
s = requests.Session()
s.headers.update({'Content-type': "application/x-www-form-urlencoded; charset=utf-8"})

username = dbutils.secrets.get(scope = "bpo-keyvault", key = "evaapiusername")
password = dbutils.secrets.get(scope = "bpo-keyvault", key = "evaapipw")

auth_body = {"client_id": "eva-cockpit", "username":username, "password":password, "grant_type":"password"}

access_token = None

def get_access_token():
    global access_token
    try:
        for attempt in Retrying(wait=wait_random_exponential(multiplier=2, max=120), stop=stop_after_attempt(1)):
            with attempt:
                r = requests.post(auth_url, auth_body)
                if r.status_code not in [200, 201]:
                    print(r)
                    raise Exception(r.status_code)
                access_token = r.json()["access_token"]
    except Exception as e:
        print(e)

# COMMAND ----------

from datetime import datetime, timedelta, timezone
import pytz

utc_dt = datetime.now(timezone.utc)
est_dt = utc_dt.astimezone(pytz.timezone("Canada/Eastern"))
est_d = est_dt.date()

look_back_period = 30

startDate = f"{(est_d - timedelta(days=look_back_period)).isoformat()}T00:00:00.000Z"
endDate = f"{est_d.isoformat()}T00:00:00.000Z"

startHour="0"
endHour="23"

(startDate,endDate)

# COMMAND ----------

import json
from typing import List, Any

data_session = requests.Session()
data_session.headers.update({'Content-type': "application/json charset=utf-8"})

channelUUIDs="cd0ae4cc-5ded-4ed0-8991-37dd811940f2"

#data_url = "https://api-americas-instance1.eva.bot/eva-dashboard/org/5f477657-7e2f-485a-95cb-196247bf97ae/env/a274da89-c104-4e2b-9186-81b4f151d96b/bot/b74fc178-ae56-463b-a793-1a40800f9f2f"
data_url =  "https://api-americas-instance2.eva.bot/eva-dashboard/org/5f477657-7e2f-485a-95cb-196247bf97ae/env/a274da89-c104-4e2b-9186-81b4f151d96b/bot/b74fc178-ae56-463b-a793-1a40800f9f2f"

# COMMAND ----------

root_api_url = "https://api-americas-instance2.eva.bot"
envuuid = "a274da89-c104-4e2b-9186-81b4f151d96b"
# 2e0ffe9b-7e64-11ee-a99b-4201ac1e000d -api
orguuid = "5f477657-7e2f-485a-95cb-196247bf97ae"
botuuid = "b74fc178-ae56-463b-a793-1a40800f9f2f"

# COMMAND ----------

#conversation_list endpoint /org/{orgUUID}/env/{envUUID}/bot/{botUUID}/conversations/list
def get_conversation_list() -> List[Any]:
    conversations = []
    try:
        page=0
        size=20
        done = False
        while not done :
            for attempt in Retrying(wait=wait_random_exponential(multiplier=2, max=120), stop=stop_after_attempt(2)):
                with attempt:
                    r = data_session.get("{host}/conversations/list?channelUUIDs={channelUUIDs}&startDate={startDate}&endDate={endDate}&startHour={startHour}&endHour={endHour}&page={page}&size={size}".format(host=data_url, channelUUIDs=channelUUIDs, startDate=startDate, endDate=endDate, startHour=startHour, endHour=endHour, page=page,size=size), 
                      headers={"Authorization" : f"Bearer {access_token}"})
                    if r.status_code == 401:
                        get_access_token()
                        r.raise_for_status()
                    output = r.json()
                    print(r)
                    print(output)
                    conversations.extend(output["content"])
                    if output['last']:
                        done = True
                    page += 1
        return conversations
    except Exception as e:
        print(e)

conversation_list = get_conversation_list()
conversation_session_codes = [{"sessionCode": x["sessionCode"], "conversationStartedAt": x["conversationStartedAt"]} for x in conversation_list]

# COMMAND ----------

#conversation_detail endpoint /org/{orgUUID}/env/{envUUID}/bot/{botUUID}/conversations/details
def get_conversation_details(args) -> Any:
    session_code = args["sessionCode"]
    try:
        for attempt in Retrying(wait=wait_random_exponential(multiplier=2, max=120), stop=stop_after_attempt(2)):
            with attempt:
                r = data_session.get("{host}/conversations/details?sessionCode={session_code}".format(host=data_url, session_code=session_code),
                    headers={"Authorization" : f"Bearer {access_token}"})
                if r.status_code == 401:
                    get_access_token()
                    r.raise_for_status()
                output = r.json()
                output["original_session_code"] = session_code
                output["conversationStartedAt"] = args["conversationStartedAt"]
                return output
    except Exception as e:
        print(e)

# COMMAND ----------

conversation_details = []

from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(10) as executor:
    for conversation_detail in executor.map(get_conversation_details, conversation_session_codes):
        conversation_details.append(conversation_detail)

assert len(conversation_details) == len(conversation_session_codes), "Details and session codes len don't match"

# COMMAND ----------

raw_df = spark.sparkContext.parallelize(conversation_details).map(lambda x: json.dumps(x))
df = spark.read.json(raw_df)

# COMMAND ----------

def channel_splitter(msg_type: str):
   return F.transform(F.filter(F.col("messages"), lambda x: F.lower(x["type"]) == F.lit(msg_type.lower())), lambda x: x["content"])
df = df.withColumn("left_channel", channel_splitter("TEXT")).withColumn("right_channel", channel_splitter("USER_INPUT")).withColumn("Date", to_date(col('conversationStartedAt')))

# COMMAND ----------

min_max = df.select(F.date_format(F.min("Date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("Date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------



target_table = "eva.eva_api_conversation"
# recreate = False
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")

df.write.partitionBy("Date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"Date >= '{min_date}' and Date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #table1

# COMMAND ----------

raw_list_df = spark.sparkContext.parallelize(conversation_list).map(lambda x: json.dumps(x))
list_df = spark.read.json(raw_list_df)

# COMMAND ----------

# list_unique = list_df.select("sessionCode").distinct().count()
# print(list_unique)

# COMMAND ----------


 flowsTravelledName = list_df.select("*", F.explode_outer("flowsTravelledNames").alias("flowsTravelledName")).drop("flowsTravelledNames")

# COMMAND ----------

flowsTravelledName_df = (flowsTravelledName.selectExpr("channel.botUuid as botUuid","channel.name as name","channel.uuid as uuid","conversationStartedAt as conversationStartedAt","durationInSeconds as durationInSeconds","flowsTravelled as flowsTravelled","satisfaction as satisfaction","sessionCode as sessionCode","userMessage as userMessage", "tags as tags",   "userMessages as userMessages","flowsTravelledName as flowsTravelledName"))

# COMMAND ----------

tag_data = flowsTravelledName_df.select("*", F.explode_outer("tags").alias("tag")).selectExpr("*", "tag.name as tag_name","tag.uuid as tag_uuid").withColumn("Date", to_date(col('conversationStartedAt')))

tag_data = tag_data.where("Date is not null")

# COMMAND ----------

min_max = tag_data.select(F.date_format(F.min("Date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("Date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------

# recreate = False

target_table = "eva.eva_api_tags"
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")

tag_data.write.partitionBy("Date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"Date >= '{min_date}' and Date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# ,"explode_outer(flowsTravelledName.tags) as tag")
#         .selectExpr("*", "tag.name","tag.uuid"))

# COMMAND ----------

# MAGIC %md
# MAGIC #table2

# COMMAND ----------


# https://api-americas-instance1.eva.bot/eva-dashboard/org/48249918-6f7e-4370-9a46-b2dc572db1a3/env/3dd72d9d-406a-4747-81fe-040556e13550/bot/205771f1-0d68-4e3b-bb0e-831eb91b95d0/messages/list?startDate=2023-10-01T00:00:00.000Z&endDate=2023-10-30T00:00:00.000Z&startHour=0&endHour=23&Page=1&Size=30
#message_list endpoint /org/{orgUUID}/env/{envUUID}/bot/{botUUID}/messages/list
def get_message_list() -> List[Any]:
    messages = []
    try:
        page=0
        size=20
        done = False
        while not done :
            for attempt in Retrying(wait=wait_random_exponential(multiplier=2, max=120), stop=stop_after_attempt(2)):
                with attempt:
                    r = data_session.get("{host}/messages/list?startDate={startDate}&endDate={endDate}&startHour={startHour}&endHour={endHour}&page={page}&size={size}".format(host=data_url, channelUUIDs=channelUUIDs, startDate=startDate, endDate=endDate, startHour=startHour, endHour=endHour, page=page,size=size), 
                      headers={"Authorization" : f"Bearer {access_token}"})
                    if r.status_code == 401:
                        get_access_token()
                        r.raise_for_status()
                    output = r.json()
                    messages.extend(output["content"])
                    if output['last']:
                        done = True
                    page += 1
        return messages
    except Exception as e:
        print(e)

message_list = get_message_list()


raw_df = spark.sparkContext.parallelize(message_list).map(lambda x: json.dumps(x))
messages_df = spark.read.json(raw_df)


# COMMAND ----------

answerName = messages_df.select("*", F.explode_outer("answerNames").alias("answerName")).drop("answerNames").withColumn("Date", to_date("deliveredAt"))

answerName = answerName.where("Date is not null")

# COMMAND ----------

min_max = answerName.select(F.date_format(F.min("Date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("Date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------

# recreate = False

target_table = "eva.eva_api_answer"
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")

answerName.write.partitionBy("Date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"Date >= '{min_date}' and Date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #table3

# COMMAND ----------

message_df = df.select("*", F.explode_outer("messages").alias("message_df")).drop("messages")

# COMMAND ----------

message_df = (message_df.selectExpr("channel.botUuid as botUuid","channel.name as name","channel.description as description","original_session_code as original_session_code","message_df  as message_df ","sessionCode as sessionCode","message_df.content as content","message_df.intent as intent","message_df.isNe as isNe","message_df.type as type","message_df.messageId as messageId","explode_outer(message_df.nlpResponses) as nlpResponse","conversationStartedAt as conversationStartedAt", "Date as Date")
        .selectExpr("*", "nlpResponse.responseType","nlpResponse.responseName"))

# COMMAND ----------

# message_df.write.mode("overwrite").saveAsTable("eva.eva_api_message")

# COMMAND ----------

min_max = message_df.select(F.date_format(F.min("Date"), "yyyy-MM-dd").alias("min_date"), 
                          F.date_format(F.max("Date"),  "yyyy-MM-dd").alias("max_date")).head()

min_date = min_max["min_date"]
max_date = min_max["max_date"]

# COMMAND ----------

# recreate = False

target_table = "eva.eva_api_message"
# if recreate:
#     spark.sql(f"DROP TABLE IF EXISTS {target_table}")
message_df.write.partitionBy("Date").format("delta").option("mergeSchema", "true").option("replaceWhere", f"Date >= '{min_date}' and Date <= '{max_date}'").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# This function converts the string cell into a date:
# from pyspark.sql.types import DateType
# tag_data = tag_data.withColumn("record_date",tag_data['conversationStartedAt'].cast(DateType()))
