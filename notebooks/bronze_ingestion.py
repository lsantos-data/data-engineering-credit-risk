# Databricks notebook source
# ==s ===========================================================
# BRONZE LAYER - Raw Ingestion
# Project: Data Engineering Credit Risk
# Description: Reads raw Lending Club data from S3 and saves
#              as Delta Table with no transformations
# Author: lsantos-data
# =============================================================

# COMMAND ----------

# Instalar a biblioteca do Kaggle
%pip install kaggle

# COMMAND ----------

import os

# Carregar credenciais do Kaggle via Databricks Secrets
os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get(scope="kaggle", key="username")
os.environ['KAGGLE_KEY'] = dbutils.secrets.get(scope="kaggle", key="api_key")

print("✅ Credenciais Kaggle carregadas do Databricks Secrets")

# COMMAND ----------

import subprocess

# Baixar o dataset do Lending Club
subprocess.run([
    'kaggle', 'datasets', 'download', 
    '-d', 'wordsforthewise/lending-club',
    '--unzip',
    '-p', '/tmp/lending-club'
])

# COMMAND ----------

import os

# Carregar credenciais do Kaggle via Databricks Secrets
# Nunca expor credenciais em código — auditável e commitável no Git
os.environ['KAGGLE_USERNAME'] = dbutils.secrets.get(scope="kaggle", key="username")
os.environ['KAGGLE_KEY'] = dbutils.secrets.get(scope="kaggle", key="api_key")

print("✅ Credenciais Kaggle carregadas do Databricks Secrets")

# COMMAND ----------

import boto3
import pandas as pd
import io

# Carregar credenciais AWS via Databricks Secrets
aws_access_key = dbutils.secrets.get(scope="aws", key="access_key_id")
aws_secret_key = dbutils.secrets.get(scope="aws", key="secret_access_key")

# Criar cliente S3 com credenciais seguras
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

# Ler o arquivo do S3 diretamente para memória
print("Lendo do S3...")
response = s3.get_object(
    Bucket='data-engineering-credit-risk-landing-ls',
    Key='raw/accepted_2007_to_2018Q4.csv'
)

# Ler apenas as primeiras 500k linhas (limitação Free Edition)
df_pandas = pd.read_csv(
    response['Body'], 
    nrows=500000,
    low_memory=False
)

print(f"✅ Linhas carregadas: {df_pandas.shape[0]:,}")
print(f"✅ Colunas: {df_pandas.shape[1]}")

# COMMAND ----------

# =============================================================
# BRONZE LAYER - Raw Ingestion
# Project: Data Engineering Credit Risk
# Description: Reads raw Lending Club data from S3 and saves
#              as Delta Table with no transformations
# Author: lsantos-data
# =============================================================

# COMMAND ----------

# Author: lsantos-data
# =============================================================

# Converter pandas para Spark e salvar como Delta Table (Bronze)
df_bronze = spark.createDataFrame(df_pandas)

print("Salvando como Delta Table...")
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_lending_club")

print(f"✅ Bronze criado com sucesso!")
print(f"Total de registros: {df_bronze.count():,}")

# COMMAND ----------

