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
            stg_df = spark.read\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["staging_dir"] + "/" + src_data)
            stg_df.createOrReplaceTempView(src_data)

        if tgt == 'REGIS_DIM':
            print("Writing REGIS_DIM  to AWS Redshift >>>>>>>")
            regis_dim_df = spark.sql(tgt_conf["loadingQuery"])
            regis_dim_df.show()
            ut.write_to_rs(regis_dim_df, "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", "DATAMART.REGIS_DIM")

        if tgt == 'CHILD_DIM':
            print("Writing CHILD_DIM  to AWS Redshift >>>>>>>")
            child_dim_df = spark.sql(tgt_conf["loadingQuery"])
            child_dim_df.show()
            ut.write_to_rs(child_dim_df, "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", "DATAMART.CHILD_DIM")

        if tgt == 'RTL_TXN_FCT':

            ut.read_from_rs("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp" , tgt_conf["target_src_table"])
            dim_df.show(5, False)

            dim_df.createOrReplaceTempView(tgt_conf["target_src_table_nm"])
            fct_tgt_df =spark.sql(tgt_conf["loadingQuery"])
            print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")
            fct_tgt_df.show(5,False)
            ut.write_to_rs(fct_tgt_df, "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp", tgt_conf["tableName"])

# spark-submit --master yarn --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/target_data_loading.py