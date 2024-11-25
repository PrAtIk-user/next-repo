# Databricks notebook source
from modules.bronze_layer import ingest_data
from modules.silver_layer import clean_data
from modules.gold_layer import aggregate_data

spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# Define paths
bronze_path = "/mnt/bronze"
silver_path = "/mnt/silver"
gold_path = "/mnt/gold"

# Execute transformations
ingest_data(spark, "/mnt/source", bronze_path)
clean_data(spark, bronze_path, silver_path)
aggregate_data(spark, silver_path, gold_path)

