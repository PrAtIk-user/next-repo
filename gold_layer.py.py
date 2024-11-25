# Databricks notebook source
def aggregate_data(spark, silver_path, gold_path):
    silver_df = spark.read.format("delta").load(silver_path)
    gold_df = silver_df.groupBy("key_column").agg({"value_column": "sum"})
    gold_df.write.format("delta").mode("overwrite").save(gold_path)
