from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import utils.utilities as ut

if __name__ == '__main__':

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    source_list = app_conf['source_list']

    for src in source_list:
        src_conf = app_conf[src]
        staging_dir = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src
        if src == 'SB':
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": src_conf["mysql_conf"]["dbtable"],
                          "numPartitions": "2",
                          "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }

            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txnDF = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load() \
                .withColumn("ins_dt", current_date())

            txnDF.show()

            txnDF.write() \
                .partitionBy("ins_dt") \
                .mode("append") \
                .parquet(staging_dir)

        if src == 'OL':
            ol_txn_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")\
                .withColumn("ins_dt", current_date())

            ol_txn_df.write()\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)

        if src == 'ADDR':
            students = spark\
                .read\
                .format("com.mongodb.spark.sql.DefaultSource")\
                .option("database", src_conf["mongodb_config"]["database"])\
                .option("collection", src_conf["mongodb_config"]["address"])\
                .load()\
                .withColumn("ins_dt", current_date())

            students.write()\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)
        if src == 'CP':
            finance_df = spark.read \
                .csv("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/finances.csv") \
                .withColumn("ins_dt", current_date())

            finance_df.write()\
                .partitionBy("ins_dt")\
                .mode("append")\
                .parquet(staging_dir)

# spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/source_data_loading.py
