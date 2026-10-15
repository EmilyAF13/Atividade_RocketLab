# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 · Job parametrizado: consulta na view
# MAGIC
# MAGIC > **Objetivo:** o Job injeta os **estados** a consultar. O notebook só lê a view e mostra o resultado.
# MAGIC
# MAGIC **Pré-aula:** execute `04_Views_Orquestracao_e_Consumo`.
# MAGIC
# MAGIC Na UI do Job: **Task → Parameters** com o mesmo nome do widget (`ufs`).

# COMMAND ----------

import pyspark.sql.functions as F

dbutils.widgets.text("ufs", "SP,RJ,MG")

ufs = [uf.strip().upper() for uf in dbutils.widgets.get("ufs").split(",") if uf.strip()]
print("Estados recebidos:", ufs)

# COMMAND ----------

catalogo = "olist_medallion"
schema_ouro = f"{catalogo}.gold"
nome_view = f"{schema_ouro}.vw_receita_estado_mes"

df_receita_estados = (
    spark.table(nome)
    .filter(F.col("uf").isin(ufs))
    .orderBy("uf", F.desc("receita_total"))
)

display(df_receita_estados)