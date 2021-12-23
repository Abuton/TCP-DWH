from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)s - Ingest Data - %(message)s", level=logging.INFO
)
logging.getLogger().setLevel(logging.INFO)
tpcdsRootDir = "datasapien_dwh"
tpcdsDdlDir = "../sql_ddl_codes"
tpcdsGenDataDir = "s3://somebucketname"
tpcdsQueriesDir = "../sql_ddl_codes/sql_queries"
tpcdsDatabaseName = "FMCG_DB"

logging.info("TPCDS root directory is at : ", tpcdsRootDir)
logging.info("TPCDS ddl scripts directory is at: ", tpcdsDdlDir)
logging.info("TPCDS data directory is at: ", tpcdsGenDataDir)
logging.info("TPCDS queries directory is at: ", tpcdsQueriesDir)


def get_spark_session():
    spark = SparkSession.builder.\
        config("spark.ui.showConsoleProgress", False).\
        config("spark.sql.autoBroadcastJoinThreshold", -1).\
        config("spark.sql.crossJoin.enabled", True).\
        getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def clear_table_directory(tableName):
    import os
    if os.path.isdir(f"spark-warehouse/fmcg.db/{tableName}/"):
        os.remove(f"spark-warehouse/fmcg.db/{tableName}")
    else:
        logging.info(f"Creating path for DWH for table {tableName}")


def create_database(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {tpcdsDatabaseName} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tpcdsDatabaseName}")
    spark.sql(f"USE {tpcdsDatabaseName}")
    logging.info(f"{tpcdsDatabaseName} Created Successfully")


#  Function to create a table in spark. It reads the DDL script for each of the
#  tpc-ds table and executes it on Spark.

def create_table(tableName: str, spark):
    clear_table_directory(tableName)
    logging.info(f"Creating table {tableName} ..")
    spark.sql(f"DROP TABLE IF EXISTS {tableName}")
    _, content = spark.sparkContext.wholeTextFiles(f"{tpcdsDdlDir}/{tableName}.sql").collect()[0]

    sqlStmts = content.replace('\n', ' ')\
        .replace("TPCDS_GENDATA_DIR", tpcdsGenDataDir)\
        .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")\
        .replace("DBNAME", "fmcg").split(";")[:-1]
    for stmt in sqlStmts:
        try:
            spark.sql(stmt.strip())
        except Exception as e:
            logging.info(f"Error Occurred {e}")


if __name__ == "__main__":
    tables = ["customer", "item", "store", "date_dim", "store_sales"]
    spark = get_spark_session()
    create_database(spark)
    for table in tables:
        create_table(table, spark)
