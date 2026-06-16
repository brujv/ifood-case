# Case Técnico iFood – Data Architecture

## Objetivo

Este projeto implementa uma solução completa de ingestão, transformação e disponibilização dos dados de corridas de táxi da cidade de Nova York (Yellow Taxi), conforme solicitado no desafio técnico para a posição de Data Architect no iFood.

A solução contempla:

* Ingestão automática dos dados de Janeiro a Maio de 2023;
* Armazenamento em Data Lake;
* Tratamento e validação dos dados;
* Disponibilização para consulta através de tabelas Delta Lake;
* Construção de camada analítica;
* Resolução das perguntas propostas no desafio.

---

# Arquitetura da Solução

```text
NYC TLC Website
       │
       ▼
┌─────────────────┐
│ Bronze Layer    │
│ Arquivos Raw    │
│ Parquet         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Silver Layer    │
│ Dados Tratados  │
│ Delta Lake      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Gold Layer      │
│ Métricas        │
│ Agregadas       │
└─────────────────┘
```

---

# Tecnologias Utilizadas

* Python
* PySpark
* Delta Lake
* Databricks Community Edition
* BeautifulSoup4
* Requests

---

# Fonte dos Dados

Os dados foram obtidos diretamente do portal oficial da NYC Taxi and Limousine Commission (TLC):

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Foram processados os datasets:

* yellow_tripdata_2023-01.parquet
* yellow_tripdata_2023-02.parquet
* yellow_tripdata_2023-03.parquet
* yellow_tripdata_2023-04.parquet
* yellow_tripdata_2023-05.parquet

---

# Camada Bronze

A camada Bronze é responsável por armazenar os arquivos originais sem qualquer transformação.

Volume criado:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.nyc_taxi_bronze
```

Local de armazenamento:

```text
/Volumes/workspace/default/nyc_taxi_bronze
```

O notebook realiza:

1. Leitura da página da TLC;
2. Identificação dos links de arquivos parquet;
3. Filtragem dos meses de interesse;
4. Download dos arquivos;
5. Persistência na camada Bronze.

---

# Camada Silver

A camada Silver é construída através da classe:

```python
NYCTaxiSilverPipeline
```

Responsabilidades:

* Leitura dos arquivos da Bronze;
* Seleção das colunas obrigatórias do desafio;
* Aplicação das regras de qualidade;
* Escrita em Delta Lake.

Tabela gerada:

```text
workspace.default.nyc_taxi_silver
```

## Colunas Mantidas

| Campo                 |
| --------------------- |
| VendorID              |
| passenger_count       |
| total_amount          |
| tpep_pickup_datetime  |
| tpep_dropoff_datetime |

Além disso é criada a coluna:

```text
pickup_year_month
```

para particionamento.

---

# Regras de Qualidade Aplicadas

Foram removidos registros que apresentavam:

* Pickup datetime nulo;
* Dropoff anterior ao pickup;
* Valor total negativo;
* Quantidade de passageiros menor ou igual a zero;
* Datas fora do período entre Janeiro e Maio de 2023.

Filtro aplicado:

```python
total_amount >= 0
passenger_count > 0
dropoff >= pickup
pickup between Jan/2023 and May/2023
```

---

# Estratégia de Particionamento

A tabela Silver é particionada por:

```text
pickup_year_month
```

Exemplo:

```text
2023-01
2023-02
2023-03
2023-04
2023-05
```

Benefícios:

* Redução de leitura de dados;
* Melhor performance de consultas;
* Escalabilidade.

---

# Camada Gold

A camada Gold contém métricas agregadas para consumo analítico.

Tabela criada:

```text
workspace.default.nyc_taxi_gold_monthly
```

Métricas disponíveis:

* monthly_revenue
* trip_count
* avg_ticket

Consulta utilizada:

```python
.groupBy("pickup_year_month")
.agg(
    sum("total_amount") as monthly_revenue,
    count(*) as trip_count,
    avg("total_amount") as avg_ticket
)
```

---

# Pergunta 1

## Qual a média de valor total recebido em um mês considerando todos os yellow táxis da frota?

Foram avaliadas duas interpretações.

### Ticket médio por corrida

```sql
SELECT
    pickup_year_month,
    AVG(total_amount) AS ticket_medio_corrida
FROM workspace.default.nyc_taxi_silver
GROUP BY pickup_year_month
```

### Média do faturamento mensal da frota

```sql
WITH faturamento_mensal AS (
    SELECT
        pickup_year_month,
        SUM(total_amount) AS faturamento_total
    FROM workspace.default.nyc_taxi_silver
    GROUP BY pickup_year_month
)

SELECT AVG(faturamento_total)
FROM faturamento_mensal
```

Foram feitas duas abordagens para a pergunta 1.
Uma considerando a média do valor por mês e a outra abordagem é o valor total médio considerando os meses analisados.


---

# Pergunta 2

## Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio?

Consulta utilizada:

```sql
WITH passageiros_por_dia_hora AS (
    SELECT
        DATE(tpep_pickup_datetime) AS dia,
        HOUR(tpep_pickup_datetime) AS hora,
        SUM(passenger_count) AS total_passageiros_na_hora
    FROM workspace.default.nyc_taxi_silver
    WHERE pickup_year_month = '2023-05'
    GROUP BY DATE(tpep_pickup_datetime),
             HOUR(tpep_pickup_datetime)
)

SELECT
    hora,
    AVG(total_passageiros_na_hora)
FROM passageiros_por_dia_hora
GROUP BY hora
ORDER BY hora
```

### Análise de dados

Da primeira query, na qual analisamos a média do valor total recebido por mês, verificamos que os valores e mantiveram constantes não apresentando uma variação significatica.

Da segunda query, na qual analisamos a média de passageiros por horário do dia, podemos concluir que os horários de pico são 17h e 18h, sendo os que horários com menos movimento são às 4h e 5h da madrugada. 

---

# Como Executar

## Instalar dependências

```bash
pip install beautifulsoup4 requests
```

## Executar o notebook

Executar as células na seguinte ordem:

1. Instalação das dependências;
2. Download dos arquivos da TLC;
3. Criação da camada Bronze;
4. Execução da classe NYCTaxiSilverPipeline;
5. Criação da camada Gold;
6. Execução das análises SQL.

---

# Estrutura do Projeto

```text
ifood-case/

├── src/
│   └── NYCTaxiSilverPipeline.py

├── analysis/
│   └── notebook.ipynb

├── README.md

└── requirements.txt
```

---
