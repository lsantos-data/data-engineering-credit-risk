# Databricks notebook source
# =============================================================
# SILVER LAYER - Data Transformation
# Project: Data Engineering Credit Risk
# Description: Reads Bronze Delta Table, cleans and standardizes
#              data, and saves as Silver Delta Table
# Author: lsantos-data
# =============================================================

# COMMAND ----------

# Ler a tabela Bronze
df_silver = spark.read.format("delta").table("bronze_lending_club")

print(f"Total de registros: {df_silver.count():,}")
print(f"Total de colunas: {len(df_silver.columns)}")

# COMMAND ----------

# Selecionar colunas relevantes para análise de crédito
colunas_relevantes = [
    # Identificação
    'id',
    # Dados do empréstimo
    'loan_amnt', 'funded_amnt', 'term', 'int_rate', 
    'installment', 'grade', 'sub_grade', 'purpose',
    # Dados do cliente
    'emp_length', 'home_ownership', 'annual_inc', 
    'verification_status', 'addr_state', 'dti',
    # Histórico de crédito
    'delinq_2yrs', 'fico_range_low', 'fico_range_high',
    'open_acc', 'pub_rec', 'revol_bal', 'revol_util',
    # Status e pagamento
    'loan_status', 'issue_d', 'last_pymnt_d',
    'total_pymnt', 'total_rec_prncp', 'total_rec_int',
    'recoveries', 'out_prncp'
]

df_silver = df_silver.select(colunas_relevantes)
print(f"Colunas selecionadas: {len(df_silver.columns)}")
print(df_silver.columns)

# COMMAND ----------

# Verificar valores nulos por coluna
from pyspark.sql.functions import col, sum as spark_sum

nulos = df_silver.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) 
    for c in df_silver.columns
])

nulos.show(vertical=True)

# COMMAND ----------

from pyspark.sql.functions import when, median

# Tratar valores nulos
df_silver = df_silver \
    .fillna({'emp_length': 'Unknown'}) \
    .fillna({'revol_util': 0.0}) \
    .fillna({'dti': 0.0}) \
    .fillna({'last_pymnt_d': 'Unknown'}) \
    .dropna(subset=['loan_amnt', 'loan_status', 'annual_inc', 'grade'])

print(f"Registros após tratamento de nulos: {df_silver.count():,}")

# COMMAND ----------

from pyspark.sql.functions import col, trim, regexp_replace, to_date

# Padronizar tipos e formatos
df_silver = df_silver \
    .withColumn('int_rate', regexp_replace(col('int_rate'), '%', '').cast('double')) \
    .withColumn('revol_util', regexp_replace(col('revol_util'), '%', '').cast('double')) \
    .withColumn('term', regexp_replace(col('term'), ' months', '').cast('integer')) \
    .withColumn('issue_d', to_date(col('issue_d'), 'MMM-yyyy')) \
    .withColumn('last_pymnt_d', when(col('last_pymnt_d') == 'Unknown', None)
                .otherwise(to_date(col('last_pymnt_d'), 'MMM-yyyy'))) \
    .withColumn('emp_length', trim(col('emp_length')))

print("Tipos padronizados com sucesso!")
df_silver.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when, col, year, month

# Criar colunas derivadas úteis para análise
df_silver = df_silver \
    .withColumn('fico_avg', (col('fico_range_low') + col('fico_range_high')) / 2) \
    .withColumn('is_default', when(col('loan_status').isin(
        'Charged Off', 'Default', 'Late (31-120 days)', 
        'Does not meet the credit policy. Status:Charged Off'
    ), 1).otherwise(0)) \
    .withColumn('issue_year', year(col('issue_d'))) \
    .withColumn('issue_month', month(col('issue_d')))

print("Colunas derivadas criadas!")
df_silver.select('fico_avg', 'is_default', 'issue_year', 'issue_month').show(5)

# COMMAND ----------

# Salvar como Delta Table (Silver)
print("Salvando como Delta Table...")
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lending_club")

print(f"✅ Silver criado com sucesso!")
print(f"Total de registros: {df_silver.count():,}")
print(f"Total de colunas: {len(df_silver.columns)}")