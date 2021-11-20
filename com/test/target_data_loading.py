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
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    jdbc_url = ut.get_redshift_jdbc_url(app_secret)
    print(jdbc_url)

    target_list = app_conf['target_list']
    for tgt in target_list:
        tgt_conf = app_conf[tgt]

        src_list = tgt_conf["source_data"]
        for src_data in src_list:

            print("\nReading data from S3 Bucket using org.apache.hadoop:hadoop-aws:2.7.4")
            stg_df = spark.read \
                    .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src_data)
            stg_df.show()
            stg_df.createOrReplaceTempView(src_data)
        if tgt == 'REGIS_DIM':
            cp_reg_tgt_df =spark.sql(tgt_conf["loadingQuery"])

            print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")

            cp_reg_tgt_df.coalesce(1).write\
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.REGIS_DIM") \
                .mode("overwrite")\
                .save()

        if tgt == 'CHILD_DIM':

            cp_chld_tgt_df = spark.sql(tgt_conf["loadingQuery"])
            cp_chld_tgt_df.show()
            print("Writing cp_tgt_df dataframe to AWS Redshift Table   CHILD_DIM >>>>>>>")
            cp_chld_tgt_df.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.CHILD_DIM") \
                .mode("overwrite") \
                .save()

        if tgt == 'RTL_TXN_FCT':

            dim_df = spark.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("query", app_conf["redshift_conf"]["query"]) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .load()

            dim_df.show(5, False)

            dim_df.createOrReplaceTempView(tgt_conf["target_src_table"])

            fct_tgt_df =spark.sql(tgt_conf["loadingQuery"])
            print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")
            fct_tgt_df.coalesce(1).write\
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", tgt_conf["tableName"]) \
                .mode("overwrite")\
                .save()

# spark-submit --master yarn --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/target_data_loading.py