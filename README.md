# Credit Risk Data Engineering Pipeline

Pipeline de dados ponta a ponta implementando arquitetura **Medallion** para análise de risco de crédito, usando o dataset público do **Lending Club**. Construído com Databricks, Delta Lake, AWS S3 e orquestrado via Lakeflow Jobs.

Projeto de portfólio focado em demonstrar práticas de engenharia de dados aplicáveis ao setor financeiro: arquitetura em camadas, modelagem dimensional, data quality, orquestração e governança de credenciais.

---

## 🏗️ Arquitetura

```
┌─────────────┐      ┌──────────┐      ┌────────────┐      ┌──────────────┐
│   Kaggle    │─────▶│  AWS S3  │─────▶│ Databricks │─────▶│   Power BI   │
│  (source)   │      │ (landing)│      │(processing)│      │(consumption) │
└─────────────┘      └──────────┘      └────────────┘      └──────────────┘
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                    ┌──────────┐        ┌──────────┐       ┌──────────┐
                    │  BRONZE  │───────▶│  SILVER  │──────▶│   GOLD   │
                    │ raw data │        │ cleansed │       │  star    │
                    │          │        │ + typed  │       │  schema  │
                    └──────────┘        └──────────┘       └──────────┘
                                                                  │
                                                                  ▼
                                                          ┌──────────────┐
                                                          │ Data Quality │
                                                          │    Checks    │
                                                          └──────────────┘
```

---

## 🛠️ Stack Tecnológica

| Camada | Ferramenta |
|---|---|
| **Compute** | Databricks (Serverless) |
| **Storage** | AWS S3 + Delta Lake |
| **Orquestração** | Databricks Lakeflow Jobs |
| **Linguagem** | Python (PySpark) + SQL |
| **Secrets Management** | Databricks Secrets |
| **Visualização** | Power BI ([dashboard executivo](docs/dashboard.html) + [medidas DAX](docs/dax_measures.md)) |
| **Versionamento** | Git / GitHub |

---

## 🔄 Pipeline de Dados

### 🥉 Bronze — Ingestão Raw

- Extração do dataset **Lending Club** via Kaggle API
- Upload para AWS S3 (camada landing) via `boto3`
- Leitura e persistência como **Delta Table** sem transformações
- **Volume:** 500.000 linhas × 151 colunas (sampled — ver [Limitações](#-limitações))

**Princípio aplicado:** Bronze preserva fidelidade absoluta ao dado original. Nenhuma transformação é aplicada, garantindo que reprocessamentos futuros possam sempre voltar à fonte.

### 🥈 Silver — Cleansing & Padronização

- Seleção de **30 colunas** relevantes para análise de risco de crédito
- Tratamento de nulls: `fillna` estratégico por tipo de coluna (`Unknown` para categóricas, `0.0` para numéricas)
- Padronização de tipos: conversão de percentuais (`int_rate`, `revol_util`), datas (`issue_d`, `last_pymnt_d`) e prazo (`term`)
- Colunas derivadas:
  - `fico_avg` = média entre `fico_range_low` e `fico_range_high`
  - `is_default` = flag binária baseada em `loan_status`
  - `issue_year`, `issue_month` = decomposição temporal

**Resultado:** 499.998 linhas × 34 colunas

### 🥇 Gold — Star Schema

Modelagem dimensional para consumo analítico:

| Tabela | Tipo | Descrição |
|---|---|---|
| `gold_fact_loan` | Fato | Métricas de empréstimo (valores, taxas, pagamentos, flag de default) |
| `gold_dim_borrower` | Dimensão | Perfil do tomador (FICO, renda, região, situação financeira) |
| `gold_dim_loan` | Dimensão | Produto (grade, sub_grade, purpose, term, status) |
| `gold_dim_date` | Dimensão | Temporal (ano, trimestre, mês, dia da semana) |

**Referential integrity** validada nas 4 tabelas via testes automatizados.

---

## ✅ Data Quality

Suite de **14 testes automatizados** rodando ao final do pipeline:

**Silver Layer (8 testes):**
- Range validation (`int_rate` entre 0-40, `fico_avg` entre 300-850)
- Domain validation (`grade` em [A-G], `term` em [36, 60])
- Not-null validation (`id`, `loan_amnt`)
- Sanity checks (`annual_inc >= 0`, `is_default` em [0, 1])

**Gold Layer (6 testes):**
- Referential integrity entre `fact_loan` e as 3 dimensões
- Uniqueness de surrogate keys em todas as dimensões

**Política:** falha em qualquer teste **bloqueia o pipeline** e dispara notificação por email. Zero retries em testes de qualidade — retry mascara problema real.

**Resultado atual:** 14/14 testes passando ✅

---

## 🎯 Orquestração (Lakeflow Jobs)

Pipeline orquestrado como **DAG** com dependências explícitas:

```
bronze_ingestion → silver_transformation → gold_star_schema → data_quality_checks
```

### Retry policy calibrada por tipo de task

| Task | Retries | Justificativa |
|---|---|---|
| `bronze_ingestion` | 3 | Ingestão sofre falhas transitórias de rede/API |
| `silver_transformation` | 1 | Transformação é determinística — retry ajuda pouco |
| `gold_star_schema` | 1 | Mesma lógica do Silver |
| `data_quality_checks` | 0 | Falha em teste é problema real, retry mascara |

### Notificações

- **On failure:** email para o responsável (ação imediata necessária)
- **On success:** desativado (evita fadiga de alerta)

---

## 🔒 Data Governance & Security

### Credential Management

Todas as credenciais (AWS, Kaggle) são gerenciadas via **Databricks Secrets**, com scopes isolados por serviço:

| Scope | Secrets |
|---|---|
| `aws` | `access_key_id`, `secret_access_key` |
| `kaggle` | `username`, `api_key` |

Credenciais **nunca aparecem em código, notebooks ou logs**. O Databricks aplica **redaction automático** — mesmo prints acidentais retornam `[REDACTED]`.

### Como o código consome secrets

```python
# Nunca hardcoded — sempre via secrets manager
aws_access_key = dbutils.secrets.get(scope="aws", key="access_key_id")
kaggle_key = dbutils.secrets.get(scope="kaggle", key="api_key")
```

### Princípios aplicados

- **Least privilege:** cada scope contém apenas as credenciais necessárias
- **No hardcoded secrets:** código auditável e commitável em repositório público
- **Separation of concerns:** credenciais separadas por domínio de serviço
- **Rotation-ready:** rotacionar credencial é atualizar o secret, sem tocar no código

### IAM (AWS)

Usuário IAM dedicado (`databricks-s3-user`) com policy customizada de menor privilégio — apenas `GetObject`, `PutObject`, `DeleteObject` e `ListBucket` no bucket específico do projeto.

---

## 📊 Volumetria

| Camada | Tabela | Linhas | Colunas |
|---|---|---|---|
| Bronze | `bronze_lending_club` | 500.000 | 151 |
| Silver | `silver_lending_club` | 499.998 | 34 |
| Gold | `gold_fact_loan` | 499.998 | 13 |
| Gold | `gold_dim_borrower` | 499.998 | 12 |
| Gold | `gold_dim_loan` | 499.998 | 5 |
| Gold | `gold_dim_date` | 15 | 8 |

---

## ⚠️ Limitações

O projeto é executado no **Databricks Free Edition**, o que impõe algumas restrições:

- **Sample de 500k linhas** (do total de ~2,26M do dataset original) devido ao limite de tamanho de arquivo no Workspace (500MB)
- **Compute exclusivamente Serverless** — não é possível provisionar clusters customizados
- **Configurações `fs.s3a` bloqueadas** — acesso ao S3 é feito via `boto3` em vez de Spark nativo
- **Retries limitados a 20** e alguns recursos avançados de Unity Catalog (column masking, row filters) não disponíveis no free tier
- **`gold_dim_date` cobre apenas 15 datas** (jan–dez/2015 e jan–mar/2018), não o período 2007–2018 completo — achado ao validar o dashboard, ver [docs/dax_measures.md](docs/dax_measures.md#2-gold_dim_date-não-cobre-2007–2018). Qualquer leitura de tendência multi-ano a partir do Gold atual precisa dessa ressalva.

Em ambiente de produção, todas essas limitações seriam eliminadas com plano Premium/Enterprise.

---

## 🗂️ Estrutura do Repositório

```
data-engineering-credit-risk/
├── notebooks/
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── gold_star_schema.py
│   └── data_quality_checks.py
├── orchestration/
│   └── lakeflow_job_definition.json
├── docs/
│   └── architecture_diagram.png
└── README.md
```

---

## 🚀 Como executar

### Pré-requisitos

- Conta Databricks (Free Edition ou superior)
- Bucket S3 configurado com IAM user de menor privilégio
- API key do Kaggle
- Databricks CLI instalado localmente

### Setup dos Secrets

```bash
# Criar scopes
databricks secrets create-scope aws
databricks secrets create-scope kaggle

# Adicionar credenciais (nunca aparecem no código)
databricks secrets put-secret aws access_key_id --string-value "<sua_key>"
databricks secrets put-secret aws secret_access_key --string-value "<sua_secret>"
databricks secrets put-secret kaggle username --string-value "<seu_user>"
databricks secrets put-secret kaggle api_key --string-value "<sua_key>"
```

### Execução

Importar os notebooks no workspace do Databricks, criar o Job com as 4 tasks encadeadas conforme documentado, e executar via **Run now** no Lakeflow Jobs.

---

## 🎓 Conceitos Demonstrados

- **Arquitetura Medallion** (Bronze / Silver / Gold)
- **Modelagem Dimensional** (Star Schema)
- **Delta Lake** (ACID, Time Travel, Schema Evolution)
- **Data Quality** automatizado com bloqueio de pipeline
- **Orquestração como DAG** com dependências e retry policy
- **Secrets Management** e governança de credenciais
- **Least Privilege** aplicado a IAM e API tokens
- **Lakehouse Architecture** unificando data lake e data warehouse

---

## 📚 Dataset

**Lending Club Loan Data** — histórico de empréstimos entre 2007-2018, com informações do tomador, produto, situação de pagamento e default. Disponível publicamente no [Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club).

---

## 👤 Autor

**Lucas Santos** — Data Analyst / Data Engineer com foco em setor financeiro
- GitHub: [@lsantos-data](https://github.com/lsantos-data)

---

## 📝 Próximos Passos

- [x] Dashboard executivo em Power BI consumindo a camada Gold
- [ ] Migração para estrutura Unity Catalog (`catalog.schema.table`)
- [ ] Implementação de column masking para colunas sensíveis (PII)
- [ ] CI/CD via GitHub Actions para deploy automatizado de notebooks
- [ ] Data lineage documentado e exportado
