import sys
from Utils.config import jdbc_url, connection_properties
from Utils.SparkSession import get_spark

spark = get_spark()


def input_table(tb_name):
    try:
        df = spark.read.jdbc(
            url = jdbc_url,
            table = tb_name,
            properties = connection_properties
        )
        row_count = df.count()
        print(f"✅ Reading {row_count} row")
        #df.show(df.count(), truncate=False)
        return df
    except Exception as e:
        print(f"❌ MSSQL Reading Error: {e}")
        spark.stop()
        sys.exit(1)

