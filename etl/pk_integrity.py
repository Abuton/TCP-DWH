from pyspark.sql import SparkSession
data_path = "spark-warehouse/tpcds1g.db"


def get_spark_session():
    spark = SparkSession.builder.\
        config("spark.ui.showConsoleProgress", True).\
        config("spark.sql.autoBroadcastJoinThreshold", -1).\
        config("spark.sql.crossJoin.enabled", True).\
        getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_table(tableName: str, spark):
    df = spark.read.parquet(f"{data_path}/{tableName}")
    return df


def check_pk_integrity(tableName: str, pk_column: str, spark):
    df = read_table(tableName, spark)
    column = df.select(pk_column)
    if column.isNull():
        print()
    else:
        print()


if __name__ == "__main__":
    spark = get_spark_session()
    df = read_table("store", spark)
    df.show(3)
