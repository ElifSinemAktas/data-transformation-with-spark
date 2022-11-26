from bronze_to_silver_data_transformation.session import *
from bronze_to_silver_data_transformation.transformation import Transformer
from datetime import datetime
import os

# Start sessions
spark = get_spark_minio_session("Dataops6-Final", os.environ['MINIO_ACCESS_KEY'],
                                os.environ['MINIO_SECRET_KEY'], "http://localhost:9000", os.environ['SPARK_HOME'])
minio_s3_res = get_s3_cli_res("minio", "http://localhost:9000")["res"]
minio_s3_cli = get_s3_cli_res("minio", "http://localhost:9000")["cli"]

# delta.tables import should be here (after starting spark session)
from delta.tables import *


def write_to_delta(df, bucket, delta, condition):
    # Control whether there is an object in silver bucket or not.
    obj_count = list(minio_s3_res.Bucket(bucket).objects.filter(Prefix=delta))
    if len(obj_count) == 0:
        df.write.format("delta").save(f"s3a://{bucket}/{delta}")
    else:
        # Read object as delta table.
        dt = DeltaTable.forPath(spark, f"s3a://{bucket}/{delta}")
        # Merge delta table with new dataframe.
        # Alias is not necessary here.
        dt.alias("dt").merge(source=df.alias("df"), condition=condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


def transform_and_save_to_silver(bronze_layer, silver_layer, dictionary, day):
    # Get all path with their prefix in a dictionary:
    # {"credits": ["credits/credits_part_20221104..., "movies" : ["movies/movies_part_20221104...]}
    prefix_key_dict = get_list_of_objects(bronze_layer, dictionary, day)[0]
    total_transaction_count = 0
    transformed_count = 0
    written_count = 0
    for prefix in prefix_key_dict.keys():
        total_count_for_prefix = len(prefix_key_dict[prefix]) * len(dictionary[prefix])
        total_transaction_count += total_count_for_prefix
        for key in prefix_key_dict[prefix]:
            df = spark.read.format("parquet").load(f"s3a://{bronze_layer}/{key}")
            for delta in dictionary[prefix]:
                # delta means cast, crew, movies, genres etc. in given dictionary
                # {"credits": ["crew", "cast"], "movies": ["movies", "genres", "keywords", "production_companies"..]}
                wlog = ""
                try:
                    df_silver, condition, delta = Transformer(df, key, delta).get_silver_df()
                    tlog = f"{delta} dataframe is created for : {key}"
                except Exception as e:
                    tlog = f"{delta} dataframe transformation is failed for : {key}"
                    #print(e)
                else:
                    transformed_count += 1
                    #print(transformed_count)
                    try:
                        write_to_delta(df_silver, silver_layer, delta, condition)
                        wlog = f"{delta} dataframe is written to delta for : {key}"
                    except Exception as e:
                        wlog = f"{delta} dataframe is not written to delta for : {key}"
                        # print(e)
                    else:
                        written_count += 1
                        # print(written_count)

                with open("log.txt", "a+") as file:
                    file.write(f"{tlog}\n{wlog}")

    if total_transaction_count == transformed_count and total_transaction_count == written_count:
        # print(total_transaction_count, transformed_count, written_count)
        return "transformation_and_save_succeeded"
    else:
        return "some_problem_occurred"


def get_list_of_objects(bronze_layer, dictionary, day):
    prefix_key_dict = {}
    total_key = 0
    for prefix in dictionary.keys():
        response = minio_s3_cli.list_objects(Bucket=bronze_layer, Prefix=prefix)
        # Get all keys/path for each prefix("credits","movies") in a list matched with given date.
        lst = [content["Key"] for content in response.get('Contents') if str(day) in content["Key"]]
        prefix_key_dict[prefix] = lst
        total_key += len(lst)
    return [prefix_key_dict, total_key]


def check_bucket(bronze_layer, dictionary, day):
    try:
        total_key = get_list_of_objects(bronze_layer, dictionary, day)[1]
    except:
        # If object storage url doesn't work
        return "http_request_error"
    else:
        if total_key > 0:
            # Transform_and_save_to_silver can be used here
            return "enough_file"
        else:
            return "not_enough_file"


# today = datetime.now().strftime("%Y%m%d")
# the_day = 20221104
# bronze = "tmdb-bronze"
# silver = "test"
# bronze_silver_dict = {"credits": ["crew", "cast"],
#                       "movies": ["movies", "genres", "keywords", "production_companies",
#                                  "production_countries", "spoken_languages"]}
#
# transform_and_save_to_silver(bronze, silver, bronze_silver_dict, the_day)
