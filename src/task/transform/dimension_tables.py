from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("tmdb") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

df_movies = spark.read.json("gs://de-porto/qoala/movies_joined.json")
df_series = spark.read.json("gs://de-porto/qoala/series_joined.json")

# Transform genres and place in GCS
df_series_gen = df_series.select(F.explode(F.col("genres"))).select(F.col("col.*")) \
    .withColumnRenamed("id", "series_id") \
    .withColumnRenamed("name", "series_genre") \
    .distinct()
df_movies_gen = df_movies.select(F.explode(F.col("genres"))).select(F.col("col.*")) \
    .withColumnRenamed("id", "movies_id") \
    .withColumnRenamed("name", "movies_genre") \
    .distinct()

genre_comparison = df_series_gen \
    .join(df_movies_gen, df_series_gen.series_id == df_movies_gen.movies_id, "full")

dwh_genres = genre_comparison \
    .withColumn("id", F.when(F.col("series_id").isNull(), F.col("movies_id")).otherwise(F.col("series_id"))) \
    .withColumn("genre", F.when(F.col("series_id").isNull(), F.col("movies_genre")).otherwise(F.col("series_genre"))) \
    .select(["id", "genre"])

dwh_genres.write.parquet("gs://de-porto/qoala/genres.parquet")


df_series_com = df_series.select(F.explode(F.col("production_companies"))).select(F.col("col.*")) \
    .select(["id", "name", "origin_country"]) \
    .withColumnRenamed("id", "series_id") \
    .withColumnRenamed("name", "series_company") \
    .withColumnRenamed("origin_country", "series_origin_country") \
    .distinct()

df_movies_com = df_movies.select(F.explode(F.col("production_companies"))).select(F.col("col.*")) \
    .select(["id", "name", "origin_country"]) \
    .withColumnRenamed("id", "movies_id") \
    .withColumnRenamed("name", "movies_company") \
    .withColumnRenamed("origin_country", "movies_origin_country") \
    .distinct()

companies_comparison = df_series_com \
    .join(df_movies_com, df_series_com.series_id == df_movies_com.movies_id, "full")

dwh_companies = companies_comparison \
    .withColumn("id", F.when(F.col("series_id").isNull(), F.col("movies_id")).otherwise(F.col("series_id"))) \
    .withColumn("company", F.when(F.col("series_id").isNull(), F.col("movies_company")).otherwise(F.col("series_company"))) \
    .withColumn("origin_country", F.when(F.col("series_id").isNull(), F.col("movies_origin_country")).otherwise(F.col("series_origin_country"))) \
    .select(["id", "company", "origin_country"]) \
    .where(F.col("origin_country").isNotNull())

dwh_companies.write.parquet("gs://de-porto/qoala/companies.parquet")

