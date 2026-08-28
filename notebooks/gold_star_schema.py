# Databricks notebook source
# =============================================================
# GOLD LAYER - Star Schema
# Project: Data Engineering Credit Risk
# Description: Reads Silver Delta Table and creates a Star Schema
#              with Fact and Dimension tables for analytics
# Author: lsantos-data
# =============================================================

# COMMAND ----------

# Ler a tabela Silver
df_silver = spark.read.format("delta").table("silver_lending_club")

print(f"Total de registros: {df_silver.count():,}")
print(f"Total de colunas: {len(df_silver.columns)}")

# COMMAND ----------

from pyspark.sql.functions import col, year, month, quarter, dayofmonth, dayofweek, date_format, to_date, expr
from pyspark.sql.types import DateType

# Criar dim_date com todas as datas únicas do Silver
dim_date = df_silver.select("issue_d").distinct() \
    .filter(col("issue_d").isNotNull()) \
    .withColumnRenamed("issue_d", "date") \
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("integer")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("day", dayofmonth(col("date"))) \
    .withColumn("day_of_week", date_format(col("date"), "EEEE")) \
    .select("date_key", "date", "year", "quarter", "month", "month_name", "day", "day_of_week")

dim_date.orderBy("date_key").show(5)
print(f"Total de datas únicas: {dim_date.count()}")

# COMMAND ----------

from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window

# Criar dim_borrower com as características do tomador
dim_borrower = df_silver.select(
    "id",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "addr_state",
    "dti",
    "delinq_2yrs",
    "fico_avg",
    "open_acc",
    "pub_rec",
    "revol_bal",
    "revol_util"
).withColumnRenamed("id", "borrower_key")

print(f"Total de tomadores: {dim_borrower.count():,}")
dim_borrower.show(3)

# COMMAND ----------

# Criar dim_loan com as características do empréstimo
dim_loan = df_silver.select(
    "id",
    "term",
    "grade",
    "sub_grade",
    "purpose",
    "loan_status"
).withColumnRenamed("id", "loan_key")

print(f"Total de empréstimos: {dim_loan.count():,}")
dim_loan.show(3)

# COMMAND ----------

from pyspark.sql.functions import date_format

# Criar fact_loan com as métricas e chaves para as dimensões
fact_loan = df_silver.select(
    col("id").alias("loan_key"),
    col("id").alias("borrower_key"),
    date_format(col("issue_d"), "yyyyMMdd").cast("integer").alias("date_key"),
    "loan_amnt",
    "funded_amnt",
    "int_rate",
    "installment",
    "total_pymnt",
    "total_rec_prncp",
    "total_rec_int",
    "recoveries",
    "out_prncp",
    "is_default"
)

print(f"Total de registros na fact: {fact_loan.count():,}")
fact_loan.show(3)

# COMMAND ----------

# Salvar todas as tabelas do Star Schema como Delta Tables

print("Salvando dim_date...")
dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_date")

print("Salvando dim_borrower...")
dim_borrower.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_borrower")

print("Salvando dim_loan...")
dim_loan.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_loan")

print("Salvando fact_loan...")
fact_loan.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_fact_loan")

print("\n✅ Star Schema criado com sucesso!")

# COMMAND ----------

# Validação: taxa de inadimplência por grade
from pyspark.sql.functions import avg, count, round as spark_round

resultado = spark.sql("""
    SELECT 
        l.grade,
        COUNT(*) as total_loans,
        ROUND(AVG(f.is_default) * 100, 2) as default_rate_pct,
        ROUND(AVG(f.int_rate), 2) as avg_interest_rate
    FROM gold_fact_loan f
    JOIN gold_dim_loan l ON f.loan_key = l.loan_key
    GROUP BY l.grade
    ORDER BY l.grade
""")

resultado.show()