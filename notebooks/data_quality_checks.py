# Databricks notebook source
# =============================================================
# DATA QUALITY CHECKS
# Project: Data Engineering Credit Risk
# Description: Automated data quality validations across all
#              layers (Bronze, Silver, Gold) to ensure data
#              integrity and business rule compliance
# Author: lsantos-data
# =============================================================

# COMMAND ----------

# Função auxiliar para rodar testes de qualidade
def run_check(check_name, condition_df, expected_count=0):
    """
    Executa um teste de qualidade.
    - check_name: nome do teste
    - condition_df: DataFrame com registros que violam a regra
    - expected_count: quantidade esperada (default 0 = zero violações)
    """
    actual_count = condition_df.count()
    status = "✅ PASS" if actual_count == expected_count else "❌ FAIL"
    print(f"{status} | {check_name}")
    print(f"       Expected: {expected_count} violations | Found: {actual_count}")
    return actual_count == expected_count

print("Função run_check pronta para uso!")

# COMMAND ----------

from pyspark.sql.functions import col

# Ler a Silver
df_silver = spark.read.format("delta").table("silver_lending_club")

print("=" * 60)
print("DATA QUALITY CHECKS - SILVER LAYER")
print("=" * 60)

# Teste 1: loan_amnt não pode ser negativo ou zero
run_check(
    "loan_amnt must be positive",
    df_silver.filter(col("loan_amnt") <= 0)
)

# Teste 2: int_rate deve estar entre 0 e 40%
run_check(
    "int_rate must be between 0 and 40",
    df_silver.filter((col("int_rate") < 0) | (col("int_rate") > 40))
)

# Teste 3: grade só pode ser A, B, C, D, E, F ou G
run_check(
    "grade must be A-G",
    df_silver.filter(~col("grade").isin(["A", "B", "C", "D", "E", "F", "G"]))
)

# Teste 4: annual_inc não pode ser negativo
run_check(
    "annual_inc cannot be negative",
    df_silver.filter(col("annual_inc") < 0)
)

# Teste 5: term deve ser 36 ou 60 meses
run_check(
    "term must be 36 or 60",
    df_silver.filter(~col("term").isin([36, 60]))
)

# Teste 6: fico_avg deve estar entre 300 e 850 (range oficial FICO)
run_check(
    "fico_avg must be between 300 and 850",
    df_silver.filter((col("fico_avg") < 300) | (col("fico_avg") > 850))
)

# Teste 7: is_default só pode ser 0 ou 1
run_check(
    "is_default must be 0 or 1",
    df_silver.filter(~col("is_default").isin([0, 1]))
)

# Teste 8: id não pode ser nulo
run_check(
    "id must not be null",
    df_silver.filter(col("id").isNull())
)

# COMMAND ----------

# Ler as tabelas do Gold
fact_loan = spark.read.format("delta").table("gold_fact_loan")
dim_borrower = spark.read.format("delta").table("gold_dim_borrower")
dim_loan = spark.read.format("delta").table("gold_dim_loan")
dim_date = spark.read.format("delta").table("gold_dim_date")

print("=" * 60)
print("DATA QUALITY CHECKS - GOLD LAYER (Referential Integrity)")
print("=" * 60)

# Teste 1: todo loan_key na fact deve existir na dim_loan
orphan_loans = fact_loan.join(dim_loan, "loan_key", "left_anti")
run_check(
    "All loan_key in fact must exist in dim_loan",
    orphan_loans
)

# Teste 2: todo borrower_key na fact deve existir na dim_borrower
orphan_borrowers = fact_loan.join(dim_borrower, "borrower_key", "left_anti")
run_check(
    "All borrower_key in fact must exist in dim_borrower",
    orphan_borrowers
)

# Teste 3: todo date_key na fact deve existir na dim_date
orphan_dates = fact_loan.join(dim_date, "date_key", "left_anti")
run_check(
    "All date_key in fact must exist in dim_date",
    orphan_dates
)

# Teste 4: loan_key deve ser único na dim_loan
duplicates_loan = dim_loan.groupBy("loan_key").count().filter(col("count") > 1)
run_check(
    "loan_key must be unique in dim_loan",
    duplicates_loan
)

# Teste 5: borrower_key deve ser único na dim_borrower
duplicates_borrower = dim_borrower.groupBy("borrower_key").count().filter(col("count") > 1)
run_check(
    "borrower_key must be unique in dim_borrower",
    duplicates_borrower
)

# Teste 6: date_key deve ser único na dim_date
duplicates_date = dim_date.groupBy("date_key").count().filter(col("count") > 1)
run_check(
    "date_key must be unique in dim_date",
    duplicates_date
)

# COMMAND ----------

# Adicionar comentários nas tabelas do Gold

spark.sql("""
    COMMENT ON TABLE gold_fact_loan IS 
    'Fact table containing loan-level metrics. Grain: one row per loan. 
     Connects to dim_loan, dim_borrower, and dim_date. 
     Used for calculating default rate, total funded amount, and interest revenue.'
""")

spark.sql("""
    COMMENT ON TABLE gold_dim_loan IS 
    'Dimension table with loan characteristics: term, grade, sub_grade, purpose, and status.
     One row per loan_key. Enables slicing metrics by loan attributes.'
""")

spark.sql("""
    COMMENT ON TABLE gold_dim_borrower IS 
    'Dimension table with borrower demographics and credit profile: employment length,
     home ownership, annual income, FICO score, DTI, and delinquency history.
     One row per borrower_key.'
""")

spark.sql("""
    COMMENT ON TABLE gold_dim_date IS 
    'Date dimension for temporal analysis. Grain: one row per loan issuance month.
     Includes year, quarter, month, and day breakdowns for time-based aggregations.'
""")

print("✅ Table comments added successfully!")

# COMMAND ----------

# Adicionar comentários nas colunas principais do fact_loan

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN loan_key 
    COMMENT 'Foreign key to gold_dim_loan. Unique identifier of the loan.'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN borrower_key 
    COMMENT 'Foreign key to gold_dim_borrower. Identifier of the borrower.'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN date_key 
    COMMENT 'Foreign key to gold_dim_date. Format: yyyyMMdd. Date the loan was issued.'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN loan_amnt 
    COMMENT 'Loan amount requested by the borrower, in USD.'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN int_rate 
    COMMENT 'Annual interest rate applied to the loan, in percent (e.g. 13.99 = 13.99%).'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN total_pymnt 
    COMMENT 'Total amount received from the borrower to date, in USD.'
""")

spark.sql("""
    ALTER TABLE gold_fact_loan ALTER COLUMN is_default 
    COMMENT 'Binary flag: 1 if the loan is in default (Charged Off, Late, etc), 0 otherwise.'
""")

print("✅ Column comments added successfully!")