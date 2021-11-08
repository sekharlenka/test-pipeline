
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