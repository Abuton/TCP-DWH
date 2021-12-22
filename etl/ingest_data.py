from pyspark.sql import SparkSession

tpcdsRootDir = "datasapien_dwh"
tpcdsWorkDir = "datasapien_dwh/work"
tpcdsDdlDir = "../sql_ddl_codes"
tpcdsGenDataDir = "../src/data"
tpcdsQueriesDir = "../src/queries"
tpcdsDatabaseName = "TPCDS1G"

print("TPCDS root directory is at : ", tpcdsRootDir)
print("TPCDS ddl scripts directory is at: ", tpcdsDdlDir)
print("TPCDS data directory is at: ", tpcdsGenDataDir)
print("TPCDS queries directory is at: ", tpcdsQueriesDir)


def get_spark_session():
    spark = SparkSession.builder.\
        config("spark.ui.showConsoleProgress", True).\
        config("spark.sql.autoBroadcastJoinThreshold", -1).\
        config("spark.sql.crossJoin.enabled", True).\
        getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def clearTableDirectory(tableName):
    import os
    if os.path.isdir(f"spark-warehouse/tpcds1g.db/{tableName}"):
        os.remove(f"spark-warehouse/tpcds1g.db/{tableName}/*")
        os.remove(f"spark-warehouse/tpcds1g.db/{tableName}")
    else:
        print("Creating path for DWH")


def createDatabase(spark):
    spark.sql(f"DROP DATABASE IF EXISTS {tpcdsDatabaseName} CASCADE")
    spark.sql(f"CREATE DATABASE {tpcdsDatabaseName}")
    spark.sql(f"USE {tpcdsDatabaseName}")
    print(f"{tpcdsDatabaseName} Created Successfully")


#  Function to create a table in spark. It reads the DDL script for each of the
#  tpc-ds table and executes it on Spark.

def createTable(tableName: str, spark):
    clearTableDirectory(tableName)
    print(f"Creating table {tableName} ..")
    spark.sql(f"DROP TABLE IF EXISTS {tableName}")
    _, content = spark.sparkContext.wholeTextFiles(f"{tpcdsDdlDir}/{tableName}.sql").collect()[0]

    sqlStmts = content.replace('\n', ' ')\
        .replace("TPCDS_GENDATA_DIR", tpcdsGenDataDir)\
        .replace("csv", "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")\
        .replace("DBNAME", "tpcds1g").split(";")[:-1]
    for stmt in sqlStmts:
        try:
            spark.sql(stmt.strip())
        except Exception as e:
            print("Error Occurred", e)


if __name__ == "__main__":
    tables = ["customer", "item", "store", "date_dim", "store_sales"]
    spark = get_spark_session()
    createDatabase(spark)
    for table in tables:
        createTable(table, spark)
