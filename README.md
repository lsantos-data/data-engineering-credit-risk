# Credit Risk Data Pipeline — Databricks Lakehouse

Pipeline de dados de ponta a ponta para análise de risco de crédito, construído sobre uma arquitetura **Lakehouse Medallion (Bronze/Silver/Gold)** no **Databricks Free Edition**, com governança via **Unity Catalog**, armazenamento em **Delta Lake**, orquestração com **Lakeflow Jobs** e visualização final em **Power BI**.

Projeto de portfólio para demonstrar competências em engenharia de dados: ingestão, modelagem dimensional, qualidade de dados, orquestração e BI.

## Objetivo

Construir um pipeline que ingere dados de empréstimos (Lending Club) e dados sintéticos complementares, transforma-os em um modelo dimensional confiável (Star Schema) e disponibiliza métricas de risco de crédito (inadimplência, exposição, perfil de tomadores) em um dashboard analítico.

## Fonte de dados

- **Lending Club Loan Data** ([Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club)) — histórico de empréstimos peer-to-peer, incluindo status de pagamento, informações de crédito e características do tomador.
- **Dados sintéticos** — gerados para enriquecer dimensões (ex.: score de crédito adicional, dados macroeconômicos, cadastro simulado de clientes) e simular cenários de teste sem expor dados sensíveis reais.

## Arquitetura

```
                 ┌─────────────────┐
  Kaggle /       │   AWS S3         │   Landing Zone
  Synthetic Data │  (raw / landing) │   (arquivos CSV/JSON brutos)
                 └────────┬─────────┘
                          │  Autoloader / ingestão
                          ▼
                 ┌─────────────────┐
                 │  BRONZE (Delta)  │   Dados brutos, schema-on-read,
                 │  Unity Catalog   │   auditoria e histórico completo
                 └────────┬─────────┘
                          │  limpeza, tipagem, deduplicação
                          ▼
                 ┌─────────────────┐
                 │  SILVER (Delta)  │   Dados validados e conformados,
                 │  Unity Catalog   │   regras de qualidade aplicadas
                 └────────┬─────────┘
                          │  modelagem dimensional
                          ▼
                 ┌─────────────────┐
                 │  GOLD (Delta)    │   Star Schema: fatos e dimensões
                 │  Unity Catalog   │   prontos para consumo analítico
                 └────────┬─────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │    Power BI      │   Dashboard de risco de crédito
                 └─────────────────┘

Orquestração: Databricks Lakeflow Jobs (agendamento e dependências entre camadas)
```

### Camadas Medallion

| Camada | Descrição |
|--------|-----------|
| **Bronze** | Ingestão bruta dos dados vindos do S3 (landing zone), preservando formato original com metadados de ingestão (timestamp, arquivo de origem). |
| **Silver** | Dados limpos, tipados corretamente, deduplicados e com regras de qualidade (nulos, domínios válidos, integridade referencial). |
| **Gold** | Modelo dimensional (Star Schema) otimizado para consultas analíticas e consumo pelo Power BI. |

### Star Schema (camada Gold)

- **Fato**: `fact_loan_performance` — grão de empréstimo/parcela, com métricas de valor, status de pagamento e inadimplência.
- **Dimensões**: `dim_borrower`, `dim_loan`, `dim_date`, `dim_credit_grade`, `dim_geography` (entre outras, conforme evolução do modelo).

## Stack tecnológica

- **AWS S3** — landing zone para dados brutos
- **Databricks Free Edition** — plataforma de processamento e engenharia de dados
- **Delta Lake** — formato de armazenamento transacional (ACID) para todas as camadas
- **Unity Catalog** — governança, catalogação e controle de acesso aos dados
- **Lakeflow Jobs** — orquestração dos pipelines (Bronze → Silver → Gold)
- **PySpark / Spark SQL** — transformações de dados
- **Power BI** — dashboard final de análise de risco de crédito

## Estrutura do repositório

```
.
├── data/           # dados de amostra locais (não versionados em grande escala)
├── docs/           # documentação complementar (diagramas, dicionário de dados)
├── notebooks/
│   ├── bronze/     # notebooks de ingestão (S3 -> Bronze)
│   ├── silver/     # notebooks de limpeza e conformação
│   └── gold/       # notebooks de modelagem dimensional (Star Schema)
├── sql/            # DDLs, criação de tabelas Unity Catalog, views
└── README.md
```

## Status do projeto

🚧 Em desenvolvimento — projeto de portfólio em construção.

## Autor

Projeto pessoal de portfólio em engenharia de dados.
