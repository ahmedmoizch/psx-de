# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog portfolio

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists portfolio.bronze;
# MAGIC create schema if not exists portfolio.silver;
# MAGIC create schema if not exists portfolio.gold;