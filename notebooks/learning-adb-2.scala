// Databricks notebook source
val userName = dbutils.secrets.get(scope = "training", key = "ADBClientSecert")

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.asif20190529.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.asif20190529.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.asif20190529.dfs.core.windows.net", "c9eb5ecb-725e-4fc5-b4d9-9c8afb36b1f4")
spark.conf.set("fs.azure.account.oauth2.client.secret.asif20190529.dfs.core.windows.net", userName)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.asif20190529.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://data@asif20190529.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://data@asif20190529.dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS radio_sample_data;
// MAGIC CREATE TABLE radio_sample_data
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC  path  "abfss://data@asif20190529.dfs.core.windows.net/small_radio_json.json"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from radio_sample_data