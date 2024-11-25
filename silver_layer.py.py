# Databricks notebook source
def clean_data(spark, bronze_path, silver_path):
    bronze_df = spark.read.format("delta").load(bronze_path)
    clean_df = bronze_df.filter("some_column IS NOT NULL")
    clean_df.write.format("delta").mode("overwrite").save(silver_path)
