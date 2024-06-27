import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, collect_list, hash

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

url = "s3://data-lake-do-pedro/Refined/"
df_csv = spark.read.parquet("s3://data-lake-do-pedro/trusted-zone/csv/")
df_tmdb = spark.read.parquet("s3://data-lake-do-pedro/trusted-zone/dt=2024-04-11/")

df_csv.createOrReplaceTempView("csv")
df_tmdb.createOrReplaceTempView("tmdb")

df_join = spark.sql("""
SELECT 
    tmdb.id,
    tmdb.titulo,
    csv.mediaimdb,
    csv.votosimdb,
    csv.nomeartista,
    tmdb.avaliacao,
    tmdb.votostmdb,
    tmdb.popularidade,
    tmdb.orcamento,
    tmdb.bilheteria,
    tmdb.data_lancamento
FROM tmdb
LEFT JOIN csv ON tmdb.id = csv.id;""")

df_join.createOrReplaceTempView("join")

df_join = df_join.withColumn("id_data", hash(df_join["data_lancamento"]))
df_join = df_join.withColumn("id_artista", hash(df_join["nomeartista"]))

df_join.createOrReplaceTempView("com_ids")

df_fato_filmes = spark.sql("""
    SELECT 
        id,
        id_data,
        avaliacao,
        bilheteria,
        orcamento,
        votostmdb,
        mediaimdb,
        votosimdb,
        popularidade
    FROM com_ids
""")
df_fato_filmes = df_fato_filmes.dropDuplicates()
 
df_dim_data = spark.sql("""
    SELECT 
        id_data,
        data_lancamento
    FROM com_ids    
""")
df_dim_data = df_dim_data.dropDuplicates()

df_dim_filme = spark.sql("""
    SELECT
        titulo,
        id
    FROM com_ids
""")
df_dim_filme = df_dim_filme.dropDuplicates()

df_dim_artista = spark.sql("""
    SELECT
        id_artista,
        nomeartista
    FROM com_ids
""")
df_dim_artista = df_dim_artista.dropDuplicates()

df_juncao_filme_artista = spark.sql("""
    SELECT
        id_artista,
        id
    FROM com_ids
    """)
df_juncao_filme_artista = df_juncao_filme_artista.dropDuplicates()


df_fato_filmes.write.parquet(url + "fato_filmes.parquet")
df_dim_filme.write.parquet(url + "dim_filme.parquet")
df_dim_data.write.parquet(url + "dim_data.parquet")
df_dim_artista.write.parquet(url + "dim_artista.parquet")
df_juncao_filme_artista.write.parquet(url + "juncao_filme_artista.parquet")

job.commit()