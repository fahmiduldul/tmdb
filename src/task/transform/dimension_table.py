from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("tmdb") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

df_movies = spark.read.json("gs://de-porto/qoala/raw_data/movies_joined.json")
df_series = spark.read.json("gs://de-porto/qoala/raw_data/series_joined.json")


def explode_and_spread_column(df, column: str):
    return df.select(F.explode(F.col(column))).select(F.col("col.*"))


def rename_columns(df, current_cols, new_cols):
    if len(current_cols) != len(new_cols):
        raise Exception()

    for i in range(len(current_cols)):
        df = df.withColumnRenamed(current_cols[i], new_cols[i])

    return df

## Transform and load genres dimenstion table
df_series_gen = rename_columns(
        explode_and_spread_column(df_series, "genres"),
        ["id", "name"], ["series_id", "series_genre"]
    ).distinct()

df_movies_gen = rename_columns(
        explode_and_spread_column(df_movies, "genres"),
        ["id", "name"], ["movies_id", "movies_genre"]
    ).distinct()

genre_comparison = df_series_gen \
    .join(df_movies_gen, df_series_gen.series_id == df_movies_gen.movies_id, "full")

dwh_genres = genre_comparison \
    .withColumn("id", F.when(F.col("series_id").isNull(), F.col("movies_id")).otherwise(F.col("series_id"))) \
    .withColumn("genre", F.when(F.col("series_id").isNull(), F.col("movies_genre")).otherwise(F.col("series_genre"))) \
    .select(["id", "genre"])

dwh_genres.write.parquet("gs://de-porto/qoala/genres.parquet")


## Transform and load production_companies dimenstion table
df_series_com = rename_columns(
        explode_and_spread_column(df_series, "production_companies"),
        ["id", "name", "origin_country"], ["series_id", "series_company", "series_origin_country"]
    ).distinct()

df_movies_com = rename_columns(
        explode_and_spread_column(df_movies, "production_companies"),
        ["id", "name", "origin_country"], ["movies_id", "movies_company", "movies_origin_country"]
    ).distinct()

companies_comparison = df_series_com \
    .join(df_movies_com, df_series_com.series_id == df_movies_com.movies_id, "full")

dwh_companies = companies_comparison \
    .withColumn("id", F.when(F.col("series_id").isNull(), F.col("movies_id")).otherwise(F.col("series_id"))) \
    .withColumn("company", F.when(F.col("series_id").isNull(), F.col("movies_company")).otherwise(F.col("series_company"))) \
    .withColumn("origin_country", F.when(F.col("series_id").isNull(), F.col("movies_origin_country")).otherwise(F.col("series_origin_country"))) \
    .select(["id", "company", "origin_country"]) \
    .where(F.col("origin_country").isNotNull())

dwh_companies.write.parquet("gs://de-porto/qoala/companies.parquet")
