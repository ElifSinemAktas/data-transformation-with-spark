from pyspark.sql import functions as F
from bronze_to_silver_data_transformation.schema import *


class Transformer:
    def __init__(self, df, key, delta):
        self.df = df
        self.key = key
        self.delta = delta

    def get_cast_crew_df(self, col):
        """Returns cast and crew dataframes and merge condition"""
        df = self.df.withColumn("cast", F.from_json(self.df.cast, schema_cast)) \
            .withColumn("crew", F.from_json(self.df.crew, schema_crew))
        df = df.select("movie_id", "title", F.explode_outer(col)).select("movie_id", "title", "col.*")
        if col == "cast":
            df = df.drop("order")

        df = df.na.fill("0000000000", ["credit_id"])

        # df.dropDuplicates(["movie_id","credit_id"])

        condition = "dt.credit_id = df.credit_id AND dt.movie_id = df.movie_id"

        return df, condition

    def get_movies_df(self):
        """Returns movies dataframe and merge condition"""
        df = self.df.select("id", "title", "budget", "homepage", "original_language", "original_title",
                       "overview", "popularity", "release_date", "revenue", "runtime", "status",
                       "tagline", "vote_average", "vote_count").withColumnRenamed("id", "movie_id")
        df = df.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
            .withColumn("budget", F.col("budget").cast(DoubleType())) \
            .withColumn("popularity", F.col("budget").cast(FloatType())) \
            .withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd")) \
            .withColumn("revenue", F.col("budget").cast(DoubleType())) \
            .withColumn("runtime", F.col("budget").cast(IntegerType())) \
            .withColumn("vote_average", F.col("vote_average").cast(FloatType())) \
            .withColumn("vote_count", F.col("vote_count").cast(IntegerType()))

        # df.dropDuplicates(["movie_id"])

        condition = "dt.movie_id = df.movie_id"

        return df, condition


    def get_genres_keywords_companies_df(self, col):
        """Returns genres, keywords and production_companies dataframes and merge condition"""
        df = self.df.select("id", col).withColumnRenamed("id", "movie_id")
        df = df.withColumn(col, F.from_json(col, schema_gkp))
        df = df.select("movie_id", F.explode_outer(col)).select("movie_id", "col.id", "col.name") \
            .withColumn("movie_id", F.col("movie_id").cast(StringType()))
        df = df.na.fill(value=-9999, subset=["id"])

        # df_silver.dropDuplicates(["movie_id", "id"])

        condition = "dt.id = df.id AND dt.movie_id = df.movie_id"

        return df, condition


    def get_languages_country_df(self, col):
        """Returns production_countries and spoken_languages and merge condition"""
        if col == "production_countries":
            schema = schema_prod_count
            condition = "dt.iso_3166_1 = df.iso_3166_1 AND dt.movie_id = df.movie_id"
            sub_col = "iso_3166_1"
        else:
            schema = schema_spok_lang
            condition = "dt.iso_639_1 = df.iso_639_1 AND dt.movie_id = df.movie_id"
            sub_col = "iso_639_1"

        df = self.df.select("id", col).withColumnRenamed("id", "movie_id")
        df = df.withColumn(col, F.from_json(col, schema))
        df = df.select("movie_id", F.explode_outer(col)).select("movie_id", f"col.{sub_col}", "col.name") \
            .withColumn("movie_id", F.col("movie_id").cast(StringType()))
        df = df.na.fill("XX", [sub_col])
        # df.dropDuplicates(["movie_id", sub_col])

        return df, condition

    def get_silver_df(self):
        if "credits" in self.key:
            df_silver, condition = self.get_cast_crew_df(self.delta)
            return df_silver, condition, self.delta
        elif "movies" in self.key:
            if self.delta == "movies":
                df_silver, condition = self.get_movies_df()
            elif self.delta == "keywords" or self.delta == "genres" or self.delta == "production_companies":
                df_silver, condition = self.get_genres_keywords_companies_df(self.delta)
            else:
                df_silver, condition = self.get_languages_country_df(self.delta)
            return df_silver, condition, self.delta
        else:
            return "There is no function to do this operation"
