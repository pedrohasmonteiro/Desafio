import requests
import json
import os
import boto3
import datetime


def lambda_handler(event, context):
    api_key = os.getenv('API_KEY_TMDB')
    aws_access_key_id = os.getenv('MY_AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('MY_AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('MY_AWS_SESSION_TOKEN')

    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token)

    def get_movies(api_key, page):
        url = "https://api.themoviedb.org/3"
      
        endpoint_discover = f'{url}/discover/movie'
        params_discover = {'api_key': api_key,
                           'with_genres': '27,9648',
                           'page': page,
                           'sort_by': 'revenue.desc'
                           }
        
        response = requests.get(endpoint_discover, params=params_discover)
        data = response.json()
        movie_ids = [movie['id'] for movie in data['results']]

        filmes = []

        for movie_id in movie_ids:
            endpoint_movie = f'{url}/movie/{movie_id}'
            params_movie = {
                'api_key': api_key
                }
            
            response_movie = requests.get(endpoint_movie, params=params_movie)
            data_movie = response_movie.json()
            filmes.append({
                'Titulo': data_movie['title'],
                'Bilheteria': data_movie['revenue'],
                'Orcamento': data_movie['budget'],
                'Avaliacao': data_movie['vote_average']
            })
        return filmes

    def save_to_s3(data, filename, bucket_name):
        current_date = datetime.datetime.now().strftime("%Y/%m/%d")
        s3_key = f"raw/moviedb/json/movie_data/{current_date}/{filename}"
        s3.put_object(Body=json.dumps(data, indent=4), Bucket=bucket_name, Key=s3_key)
            
    bucket_name = 'data-lake-do-pedro'
    total_pages = 10  
    movies_per_file = 20 

    for page in range(1, total_pages + 1):
        movies = get_movies(api_key, page)
        for i in range(0, len(movies), movies_per_file):
            chunk = movies[i:i+movies_per_file]
            filename = f"movies_{page}_{i // movies_per_file}.json"
            save_to_s3(chunk, filename, bucket_name)
