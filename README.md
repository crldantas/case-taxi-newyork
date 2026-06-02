# Case Técnico Data Architect — iFood
## Ingestão e Análise de Dados de Táxis de Nova York (Jan–Mai 2023)

**Autor:** Carlos Eduardo Dantas
**Data:** Junho/2026

---

## Objetivo

Solução de engenharia de dados para ingestão, transformação e disponibilização dos dados de corridas de táxi amarelo de NY (NYC TLC), referentes ao período de janeiro a maio de 2023.

---

## Arquitetura

```
NYC TLC Website
      ↓
  S3 landing/          ← arquivos parquet originais
      ↓ (AWS Glue — PySpark)
  S3 silver/           ← todos os campos + row_key (schema padronizado)
      ↓ (AWS Glue — PySpark)
  S3 gold/             ← 5 colunas obrigatórias + row_key (dados limpos e deduplicados)
      ↓ (Databricks notebook — pandas + Spark)
  Databricks serving   ← tabela Delta para consumo via SQL
      ↓
  Análises SQL         ← respostas às perguntas do case
```

### Tecnologias utilizadas

- **AWS S3** — Data Lake (landing, silver, gold)
- **AWS Glue 5.1 com PySpark** — ETL e transformação (requisito do case: uso de PySpark)
- **Databricks Free Edition** — consumo via SQL e análise final
- **Delta Lake** — formato de tabela na camada serving

---

## Estrutura do Repositório

```
ifood-case/
├── src/
│   ├── glue_01_ingestion_landing.py   # Download NYC TLC → S3 landing
│   ├── glue_02_landing_to_silver.py   # landing → silver (schema padronizado)
│   ├── glue_03_silver_to_gold.py      # silver → gold (5 colunas, limpo)
│   └── 04_serving_load.py             # gold → Databricks serving (notebook)
├── analysis/
│   └── 05_analysis_queries.py         # Queries SQL com respostas do case (notebook)
└── README.md
```

---

## Camadas do Data Lake

| Camada | Localização | Descrição |
|--------|-------------|-----------|
| **Landing** | `s3://case-taxi-ny-dados/landing/` | Arquivos parquet originais do NYC TLC |
| **Silver** | `s3://case-taxi-ny-dados/silver/` | Todos os campos + `row_key` + `year` + `month` com tipos padronizados |
| **Gold** | `s3://case-taxi-ny-dados/gold/` | 5 colunas obrigatórias + `row_key`, dados limpos e deduplicados |
| **Serving** | `serving.yellow_tripdata_ny` (Databricks) | Tabela Delta para consulta SQL |

---

## Colunas obrigatórias (camada Gold e Serving)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `VendorID` | LONG | Identificador do fornecedor |
| `passenger_count` | LONG | Número de passageiros |
| `total_amount` | DOUBLE | Valor total da corrida |
| `tpep_pickup_datetime` | TIMESTAMP | Data/hora de embarque |
| `tpep_dropoff_datetime` | TIMESTAMP | Data/hora de desembarque |

---

## Regras de qualidade aplicadas (Silver → Gold)

- Remoção de registros com valores nulos nas 5 colunas obrigatórias
- `passenger_count > 0`
- `total_amount > 0`
- `tpep_pickup_datetime < tpep_dropoff_datetime`
- Deduplicação por `row_key` (MD5 de VendorID + pickup + dropoff)
- Filtro de período: ano 2023, meses 1 a 5

---

## Como executar

### Pré-requisitos

- Conta AWS com bucket S3 `case-taxi-ny-dados` (região `us-east-2`)
- IAM Role com permissão `AmazonS3FullAccess` para o Glue
- Conta Databricks (Free Edition)

### 1. Ingestão — Download para Landing

Crie um AWS Glue Job com o script `glue_01_ingestion_landing.py`.

- **Glue version:** 4.0
- **IAM Role:** `glue-case-taxi-role`

### 2. Transformação Landing → Silver (PySpark)

Crie um AWS Glue Job com o script `glue_02_landing_to_silver.py`.

- **Glue version:** 5.1
- **Type:** Spark
- **IAM Role:** `glue-case-taxi-role`

### 3. Transformação Silver → Gold (PySpark)

Crie um AWS Glue Job com o script `glue_03_silver_to_gold.py`.

- **Glue version:** 5.1
- **Type:** Spark
- **IAM Role:** `glue-case-taxi-role`

### 4. Carga no Databricks

Importe o notebook `src/04_serving_load.py` no Databricks, preencha as credenciais AWS nos widgets e execute.

### 5. Análises

Importe o notebook `analysis/05_analysis_queries.py` no Databricks e execute para obter as respostas.

---

## Respostas às perguntas do case

### Pergunta 1 — Média de total_amount por mês

```sql
SELECT
    MONTH(tpep_pickup_datetime)    AS month,
    ROUND(AVG(total_amount), 2)    AS avg_total_amount,
    COUNT(*)                       AS total_trips
FROM serving.yellow_tripdata_ny
GROUP BY MONTH(tpep_pickup_datetime)
ORDER BY month
```

### Pergunta 2 — Média de passenger_count por hora do dia (maio/2023)

```sql
SELECT
    HOUR(tpep_pickup_datetime)         AS hour,
    ROUND(AVG(passenger_count), 4)     AS avg_passenger_count
FROM serving.yellow_tripdata_ny
WHERE MONTH(tpep_pickup_datetime) = 5
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hour
```

---

## Fonte dos dados

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)