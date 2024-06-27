import boto3
import os
from datetime import datetime


#aws_access_key_id = 
#aws_secret_access_key = 
#aws_session_token = 


def carregar_para_s3(file_path, bucket_name, object_key):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token
)
    s3.upload_file(file_path, bucket_name, object_key)


def generate_s3_key(file_name, data_source, data_format):
    current_date = datetime.now().strftime('%Y/%m/%d')
    return f"RAW_Zone/{data_source}/{data_format}/original/{current_date}/{file_name}"


def main():
  
    bucket_name = 'data-lake-do-pedro'
    movies_csv = '/app/movies.csv'
    series_csv = '/app/series.csv'

    # enviando arquivo para o S3
    for file_path, data_source in [(movies_csv, 'filmes'), (series_csv, 'series')]:
        file_name = os.path.basename(file_path)
        s3_key = generate_s3_key(file_name, data_source, 'csv')
        carregar_para_s3(file_path, bucket_name, s3_key)
        print(f"Arquivo {file_name} enviado para o S3 em {s3_key}")
        

if __name__ == "__main__":
    main()
