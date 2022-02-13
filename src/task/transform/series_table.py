from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("tmdb") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

df_series = spark.read.json("gs://de-porto/qoala/series_joined.json")

initial_columns = df_series.schema.names

dwh_series = df_series \
    .withColumn("genre", F.explode("genres")) \
    .select(*initial_columns, F.col("genre.id").alias("genre_id")) \
    .groupBy(*initial_columns).agg(F.collect_list("genre_id").alias("genre_ids")) \
    .withColumn("company", F.explode("production_companies")) \
    .select(*initial_columns, "genre_ids", F.col("company.id").alias("company_id")) \
    .groupBy(*initial_columns, "genre_ids").agg(F.collect_list("company_id").alias("company_ids")) \
    .drop("genres", "production_companies")

dwh_series.write.parquet("gs://de-porto/qoala/series.parquet")
