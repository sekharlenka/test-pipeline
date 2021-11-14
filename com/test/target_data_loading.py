from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import utils.utilities as ut

if __name__ == '__main__':

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    target_list = app_conf['target_list']
    for tgt in target_list:
        tgt_conf = app_conf[tgt]
        if tgt == 'REGIS_DIM':
            src_data = tgt_conf["source_data"]
            print("\nReading data from S3 Bucket using org.apache.hadoop:hadoop-aws:2.7.4")
            cp_df = spark.read \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src_data)

            cp_df.createOrReplaceTempView("CP")

            spark.sql(tgt_conf["loadingQuery"]).show()






    # spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/trget_data_loading.py