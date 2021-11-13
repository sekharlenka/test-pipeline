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

    source_list = app_conf['source_list']
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    for src in source_list:
        src_conf = app_conf[src]
        staging_dir = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src
        if src == 'SB':
            txn_df = ut.read_from_mysql(app_secret, src_conf["mysql_conf"]["dbtable"], src_conf["mysql_conf"]["partition_column"], spark)

            txn_df = txn_df.withColumn("ins_dt", current_date())
            txn_df.show()

            txn_df.write \
                .partitionBy("ins_dt") \
                .mode("append") \
                .parquet(staging_dir)

        if src == 'OL':
            ol_txn_df = ut.read_from_sftp(app_secret, src_conf["sftp_conf"]["directory"], src_conf["sftp_conf"]["filename"], os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]), spark )
            ol_txn_df2 = ol_txn_df.withColumn("ins_dt", current_date())
            ol_txn_df2.show()

            ol_txn_df2.write\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)

        if src == 'ADDR':
            students = spark\
                .read\
                .format("com.mongodb.spark.sql.DefaultSource")\
                .option("database", src_conf["mongodb_config"]["database"])\
                .option("collection", src_conf["mongodb_config"]["collection"])\
                .load()\
                .withColumn("ins_dt", current_date())


            students.write\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)
            print("\nReading data from MongoDB using com.mongodb")
            students.count()
        if src == 'CP':
            finance_df = spark.read \
                .csv("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/finances.csv") \
                .withColumn("ins_dt", current_date())

            finance_df.write\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)

# spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/source_data_loading.py
