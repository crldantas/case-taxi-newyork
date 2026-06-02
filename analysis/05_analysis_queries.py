# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise: Respostas às perguntas do case iFood
# MAGIC
# MAGIC ## Pergunta 1
# MAGIC Qual a média de valor total (total_amount) recebido em um mês
# MAGIC considerando todos os yellow taxis da frota?
# MAGIC
# MAGIC ## Pergunta 2
# MAGIC Qual a média de passageiros (passenger_count) por cada hora do dia
# MAGIC que pegaram táxi no mês de maio considerando todos os taxis da frota?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Média de total_amount por mês

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MONTH(tpep_pickup_datetime)    AS month,
# MAGIC     ROUND(AVG(total_amount), 2)    AS avg_total_amount,
# MAGIC     COUNT(*)                       AS total_trips
# MAGIC FROM serving.yellow_tripdata_ny
# MAGIC GROUP BY MONTH(tpep_pickup_datetime)
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Média de passenger_count por hora do dia (maio/2023)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     HOUR(tpep_pickup_datetime)         AS hour,
# MAGIC     ROUND(AVG(passenger_count), 4)     AS avg_passenger_count
# MAGIC FROM serving.yellow_tripdata_ny
# MAGIC WHERE MONTH(tpep_pickup_datetime) = 5
# MAGIC GROUP BY HOUR(tpep_pickup_datetime)
# MAGIC ORDER BY hour
