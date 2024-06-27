from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, collect_list 


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("historical-csv-load")

input_path = "s3://data-lake-do-pedro/RAW_Zone/filmes/csv/original/2024/04/09/"
output_path = "s3://data-lake-do-pedro/trusted-zone/"

df = spark.read.option("sep", "|").csv(input_path, header=True, inferSchema=True)


df = df.select(col("id"), col("tituloOriginal").alias("titulo"), col("notaMedia").alias("MediaIMDB"),col("numeroVotos").alias("VotosIMDB"), col("nomeArtista"))

df = df.filter(((col("genero").contains("Horror")) | (col("genero").contains("Mystery"))) & (col("anoLancamento") >= 2000))

df = df.dropDuplicates()

df = df.na.drop() 

df.show()

df.write.mode("append").parquet(output_path)

job.commit()
