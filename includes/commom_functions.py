# Databricks notebook source
from pyspark.sql.functions import current_timestamp
# function to add ingestion date collumn in the dataframe
def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())
