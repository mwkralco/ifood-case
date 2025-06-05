import boto3
import requests
import pandas as pd
from io import BytesIO

# Variáveis
bucket_name = 'nyc-trip-data-s3'
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
months = ['01', '02', '03', '04', '05']
year = '2023'
vehicle_types = ['yellow']

aws_access_key = 'AWS_ACCESS_KEY'
aws_secret_key = 'AWS_SECRET_KEY'
region_name = 'us-east-2'

s3_client = boto3.client('s3',
                  aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_key,
                  region_name=region_name)

# Schema padrão
schema = {
    'VendorID': 'int',
    'tpep_pickup_datetime': 'timestamp',
    'tpep_dropoff_datetime': 'timestamp',
    'passenger_count': 'int',
    'trip_distance': 'double',
    'RatecodeID': 'double',
    'store_and_fwd_flag': 'string',
    'PULocationID': 'int',
    'DOLocationID': 'int',
    'payment_type': 'int',
    'fare_amount': 'double',
    'extra': 'double',
    'mta_tax': 'double',
    'tip_amount': 'double',
    'tolls_amount': 'double',
    'improvement_surcharge': 'double',
    'total_amount': 'double',
    'congestion_surcharge': 'double',
    'airport_fee': 'double',
    'lpep_pickup_datetime': 'timestamp',
    'lpep_dropoff_datetime': 'timestamp'
}

for vehicle in vehicle_types:
    for month in months:
        filename = f'{vehicle}_tripdata_{year}-{month}.parquet'
        url = base_url + filename

        # Particionamento: year=YYYY/month=MM/
        s3_key = f'bronze/{vehicle}_tripdata/year={year}/month={month}/part.parquet'

        print(f'Downloading {url} ...')
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            print(f'Processing {filename} ...')

            try:
                parquet_file = BytesIO(response.content)
                df = pd.read_parquet(parquet_file)

                # Garante colunas do schema
                for col, col_type in schema.items():
                    if col not in df.columns:
                        df[col] = pd.NA
                        df[col] = df[col].astype('string')
                    else:
                        if col_type == 'int':
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif col_type == 'double':
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif col_type == 'timestamp':
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        elif col_type == 'string':
                            df[col] = df[col].astype('string')

                # Remove colunas extras
                df = df[list(schema.keys())]

                # Converte para Parquet em memória
                out_buffer = BytesIO()
                df.to_parquet(out_buffer, index=False)
                out_buffer.seek(0)

                # Upload para o S3
                print(f'Uploading Parquet to s3://{bucket_name}/{s3_key} ...')
                s3_client.upload_fileobj(out_buffer, bucket_name, s3_key)
                print(f'Uploaded {s3_key}')

            except Exception as e:
                print(f'Failed to process {filename}: {e}')
        else:
            print(f'Failed to download {url}, status: {response.status_code}')
