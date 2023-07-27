# Databricks notebook source
# read file in s3
import json

s3_df = spark.read.format("json").load(stage_area)

 

# save schema

with open("/dbfs/users/hotmart/schemas/schema1.json", "w") as f:
  json.dump(s3_df.schema.jsonValue(), f)


# view schema in file

with open("/dbfs/users/hotmart/schemas/schema1.json", "r") as f_read:
  for line in f_read:
    print(line)

