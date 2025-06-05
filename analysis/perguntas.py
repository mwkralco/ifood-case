paths = [f's3a://{bucket}/gold/yellow_tripdata/year={year_val}/month={m}/' for m in months]
df = spark.read.parquet(*paths)
df.createOrReplaceTempView("yellow_tripdata")

result = spark.sql("""
    SELECT 
        YEAR(tpep_pickup_datetime) AS year,
        MONTH(tpep_pickup_datetime) AS month,
        AVG(total_amount) AS avg_total_amount, 
        COUNT(*) AS trip_count
    FROM yellow_tripdata
    GROUP BY 1,2
    ORDER BY 1,2
""")
result.show()

result = spark.sql("""
    SELECT 
        HOUR(tpep_pickup_datetime) AS pickup_hour,
        AVG(passenger_count) AS avg_passenger_count
    FROM yellow_tripdata
    WHERE MONTH(tpep_pickup_datetime) = 5
    GROUP BY pickup_hour
    ORDER BY pickup_hour
""")
result.show()
