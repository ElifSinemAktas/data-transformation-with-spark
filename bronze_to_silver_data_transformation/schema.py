from pyspark.sql.types import *

schema_cast = (
    ArrayType(
        StructType([
            StructField("cast_id", IntegerType()),
            StructField("character", StringType()),
            StructField("credit_id", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("order", IntegerType())
                  ])
            )
         )

schema_crew = (ArrayType(
        StructType([
            StructField("credit_id", StringType()),
            StructField("department", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("job", StringType()),
            StructField("name", StringType())
                  ])
            )
         )

schema_gkp = (
    ArrayType(
        StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
                  ])
            )
         )

schema_prod_count = (
    ArrayType(
        StructType([
            StructField("iso_3166_1", StringType()),
            StructField("name", StringType()),
        ])
    )
)

schema_spok_lang = (
    ArrayType(
        StructType([
            StructField("iso_639_1", StringType()),
            StructField("name", StringType()),
        ])
    )
)
