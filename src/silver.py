from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Configurações AWS
aws_access_key = "SEU_ACCESS_KEY"
aws_secret_key = "SEU_SECRET_KEY"

# Cria SparkSession
builder = SparkSession.builder.appName("NYC Yellow Tripdata Processing")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configura credenciais para acessar S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

# Parâmetros
bucket = 'nyc-trip-data-s3'
year_val = '2023'
months = ['01', '02', '03', '04', '05']

for m in months:
    print(f'Processing Silver for month {m}...')

    path_bronze = f's3a://{bucket}/bronze/yellow_tripdata/year={year_val}/month={m}/'
    path_silver = f's3a://{bucket}/silver/yellow_tripdata/'

    # Leitura dos dados bronze
    df = spark.read.parquet(path_bronze)

    # Garante que datetime está no tipo timestamp
    df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))

    # Cria colunas year e month para particionamento
    df = df.withColumn("year", year("tpep_pickup_datetime")) \
           .withColumn("month", month("tpep_pickup_datetime"))

    # Filtra só o mês e ano correto
    df = df.filter((col("year") == int(year_val)) & (col("month") == int(m)))

    # Escreve dados na camada silver, particionando por year e month
    df.write.format("parquet") \
        .mode("append") \
        .partitionBy("year", "month") \
        .option("overwriteSchema", "true") \
        .save(path_silver)

    print(f'Silver layer saved for month {m} at {path_silver}')
