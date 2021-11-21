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
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    source_list = app_conf['source_list']

    for src in source_list:

        src_conf = app_conf[src]
        staging_dir = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src
        if src == 'SB':
            txn_df = ut.read_from_mysql(app_secret, src_conf["mysql_conf"]["dbtable"], src_conf["mysql_conf"]["partition_column"], spark)
            txn_df = txn_df.withColumn("ins_dt", current_date())
            txn_df.show()

            # write a function to write a df to s3
            ut.write_to_s3(txn_df, staging_dir)

        if src == 'OL':
            ol_txn_df = ut.read_from_sftp(app_secret, src_conf["sftp_conf"]["directory"], src_conf["sftp_conf"]["filename"], os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]), spark )
            ol_txn_df2 = ol_txn_df.withColumn("ins_dt", current_date())
            ol_txn_df2.show()

            # write a function to write a df to s3
            ut.write_to_s3(ol_txn_df2, staging_dir)

        if src == 'ADDR':
            # write a function to read from mongo db
            addr_df = ut.read_from_mongodb(spark, src_conf["mongodb_config"]["database"], src_conf["mongodb_config"]["collection"]) \
                .withColumn("ins_dt", current_date())
            addr_df.show(5)
            ut.write_to_s3(addr_df, staging_dir)

        if src == 'CP':
            print("\nReading data from S3 Bucket using org.apache.hadoop:hadoop-aws:2.7.4")
            # write a function to read from s3
            cp_df = ut.read_csv_from_s3(spark, "s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/" + src_conf["s3_conf"]["filename"])\
                .withColumn("ins_dt", current_date())

            ut.write_to_s3(cp_df, staging_dir)

# spark-submit --master yarn --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/source_data_loading.py
