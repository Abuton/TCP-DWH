from ingest_data import get_spark_session, createDatabase
import logging
from pprint import pprint

logging.basicConfig(
    format="%(asctime)s %(levelname)s - PK Consistency Check - %(message)s",
    level=logging.INFO,
)
logging.getLogger().setLevel(logging.INFO)
data_path = "spark-warehouse/tpcds1g.db"


def read_table(tableName: str, spark):
    df = spark.read.parquet(f"{data_path}/{tableName}")
    pprint(df.printSchema())
    return df


def add_pk_constraint(spark, tableName: str, pkColumn: str):
    stmt = f"alter table {tableName} add primary key ({pkColumn});"
    try:
        spark.sql(stmt)
    except Exception as e:
        print("An Error Occurred", e)


if __name__ == "__main__":
    spark = get_spark_session()
    createDatabase(spark)
    tables_pk_column = {
        "customer": "c_customer_sk",
        "item": "i_item_sk",
        "store": "s_store_sk",
        "date_dim": "d_date_sk",
        "store_sales": "ss_sold_date_sk",
    }
    for table in tables_pk_column.keys():
        add_pk_constraint(spark, table, tables_pk_column[table])
