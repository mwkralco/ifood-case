from pyspark.sql.functions import col, year, month

year_val = '2023'
months = ['1', '2', '3', '4', '5']
bucket = 'nyc-trip-data-s3'

for m in months:
    path_silver = f's3a://{bucket}/silver/yellow_tripdata/year={year_val}/month={m}/'
    path_gold = f's3a://{bucket}/gold/yellow_tripdata/'

    print(f'Processing Gold for month {m} from Silver at {path_silver}...')

    # Lê apenas a partição correta
    df = spark.read.format("parquet").load(path_silver)

    selected_cols = ['VendorID', 'passenger_count', 'tpep_pickup_datetime',
                     'tpep_dropoff_datetime', 'total_amount', 'year', 'month']

    df = df.select(*[c for c in selected_cols if c in df.columns])

    # Escreve como Parquet, adicionando partições
    df.write.format("parquet") \
        .mode("append") \
        .partitionBy("year", "month") \
        .save(path_gold)

    print(f'Gold saved for month {m} at {path_gold}')
