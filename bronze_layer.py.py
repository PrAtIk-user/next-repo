# Databricks notebook source
def ingest_data(spark, source_path, bronze_path):
    raw_df = spark.read.csv(source_path, header=True, inferSchema=True)
    raw_df.write.format("delta").mode("overwrite").save(bronze_path)
