# NYC Taxi Data Pipeline

Este projeto implementa um **pipeline de dados** baseado na modelagem **Medalhão (Bronze, Silver, Gold)**, utilizando:

✅ **Apache Spark** no **Databricks Free Tier**  
✅ **Armazenamento em Parquet** diretamente no **AWS S3 Free Tier**

O objetivo é processar os dados públicos de corridas de táxi de Nova York, organizando-os em camadas que melhoram a **estrutura**, **qualidade** e **eficiência** das análises.

## Tecnologias Utilizadas

- **Apache Spark** (via Databricks Community Edition - gratuito)
- **AWS S3** (Free Tier)
- **Parquet** como formato de armazenamento
- **boto3** para interações com S3
- **pandas** para manipulação tabular
- **requests** para APIs externas

## Estrutura do Pipeline — Modelagem Medalhão

O pipeline segue a modelagem **Medalhão**, uma arquitetura de referência que organiza o fluxo de dados em três camadas:

### 🥉 Bronze — Dados Brutos

- Armazenamento fiel dos dados recebidos, em formato **Parquet**.
- Nenhuma transformação é aplicada, garantindo **rastreabilidade** e possibilidade de **reprocessamento**.

### 🥈 Silver — Dados Limpos

- Transformações básicas: tipagens corretas, criação de colunas auxiliares e partições para consultas eficientes.
- Filtragem de inconsistências.

### 🥇 Gold — Dados Filtrados

- Somente os dados relevantes para **análise**.
- Otimiza o consumo por **dashboards**, **relatórios** e **Data Science**.

## Por que usar a modelagem Medalhão?

✅ **Rastreabilidade:** cada camada documenta uma etapa do processamento.  
✅ **Qualidade:** permite transformar, validar e enriquecer os dados em etapas.  
✅ **Eficiência:** consultas analíticas podem ser feitas diretamente na camada **Gold**, mais enxuta e otimizada.  
✅ **Manutenção:** facilita **reprocessamentos parciais** sem afetar dados finais.

## Configuração

### Requisitos

- Conta gratuita no **Databricks Community Edition**
- **AWS S3** configurado (Free Tier)
- **Python** >= 3.7
- **Spark** >= 3.0

### Passos

1. Configure as credenciais AWS no Databricks:  
   - AWS_ACCESS_KEY_ID 
   - AWS_SECRET_ACCESS_KEY

2. Ajuste as variáveis no código:  
   - bucket: nome do bucket S3  
   - year_val: ano desejado  
   - months: lista de meses a processar

## Armazenamento no S3

O pipeline cria as seguintes estruturas:
- s3a://<bucket>/bronze/yellow_tripdata/year=<YYYY>/month=<MM>/
- s3a://<bucket>/silver/yellow_tripdata/year=<YYYY>/month=<MM>/
- s3a://<bucket>/gold/yellow_tripdata/year=<YYYY>/month=<MM>/

Todas as camadas armazenam arquivos **Parquet**, aproveitando a **compactação** e a **leitura eficiente** desse formato.


## Exemplo de consulta analítica no Spark SQL

```sql
SELECT 
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    AVG(passenger_count) AS avg_passengers
FROM yellow_tripdata
WHERE year = 2023 AND month = 5
GROUP BY 1
ORDER BY 1;
```
# Autor
Desenvolvido por Mariana Kralco.



