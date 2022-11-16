import boto3
from pyspark.sql import SparkSession
import findspark


def get_s3_cli_res(profile, url):
    session = boto3.Session(profile_name=profile)
    s3_cli = session.client("s3", endpoint_url=url)
    s3_res = session.resource("s3", endpoint_url=url)

    return {"cli": s3_cli, "res": s3_res}


def get_spark_minio_session(name, access_key, secret_key, url):
    findspark.init("/opt/manual/spark")

    spark = (SparkSession.builder
             .appName(name)
             .master("local[2]")
             .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", url)
             # .config("spark.sql.debug.maxToStringFields", 500)
             # .config("spark.driver.memory", "3g")
             .getOrCreate())

    return spark