
def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def read_from_mysql(app_secret, table, partition_col, spark):
    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    jdbc_params = {"url": get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": table,
                   "numPartitions": "2",
                   "partitionColumn": partition_col,
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
    txn_df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()
    return txn_df

def read_from_mongodb(spark, database, collection):
    df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", database) \
        .option("collection", collection) \
        .load()
    return df


def read_from_sftp(app_secret, directory, filename, pemfile, spark):
    print("\nReading data from sftp using com.springml.spark.sftp")
    ol_txn_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", pemfile)\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(directory + "/" + filename)
    return ol_txn_df

def read_csv_from_s3(spark, path, delimiter = '|', header = 'true'):
    df = spark.read \
        .option('delimiter', delimiter) \
        .option('header', header) \
        .csv(path)
    return df

def write_to_s3(df, staging_dir, partition_col = 'ins_dt', mode = 'overwrite'):
    df.write \
        .partitionBy(partition_col) \
        .mode(mode) \
        .parquet(staging_dir)

def write_to_rs (df, path, dbtable, jdbc_url):
    df.coalesce(1).write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("tempdir", path) \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", dbtable) \
        .mode("overwrite")\
        .save()

def read_from_rs (spark,path,target_table, jdbc_url):
    df = spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbc_url) \
        .option("dbtable", target_table) \
        .option("forward_spark_s3_credentials", "true") \
        .option("tempdir", path) \
        .load()
    return df

