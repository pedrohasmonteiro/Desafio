from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract, lit, concat

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("tmdb-api-load")

input_path = "s3://data-lake-do-pedro/RAW_Zone/moviedb/json/movie_data/2024/04/08/"
output_path = "s3://data-lake-do-pedro/trusted-zone/"

df = spark.read.json(input_path, multiLine=True)

date_pattern = r"(\d{4})/(\d{2})/(\d{2})/"
  
df = df.withColumn("dt", concat(regexp_extract(lit(input_file_name()), date_pattern, 1), lit("-"),
                                regexp_extract(lit(input_file_name()), date_pattern, 2), lit("-"),
                                regexp_extract(lit(input_file_name()), date_pattern, 3)))

df.write.partitionBy("dt").mode("overwrite").parquet(output_path)

job.commit()
