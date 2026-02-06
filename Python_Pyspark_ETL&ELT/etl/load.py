import sys
from Utils.config import POSTGRES_URL, POSTGRES_PROPERTIES
from Utils.SparkSession import get_spark

spark = get_spark()


# '''POSTGRES_TABLE = bronze_l.sales_salesterritory # სქემა + ტაბელი'''
def writing_data_to_postgresql(df_to_insert, POSTGRES_TABLE):
    WRITE_MODE = "overwrite"  # "overwrite" ან "append"
    try:
        (df_to_insert.write
            .option("stringtype", "unspecified")   # <-- UUID-ისთვის აუცილებელი
            .jdbc(
                url=POSTGRES_URL,
                table=POSTGRES_TABLE,
                mode=WRITE_MODE,
                properties=POSTGRES_PROPERTIES
            ))
        print("✅ ჩაწერა წარმატებით დასრულდა!")
    except Exception as e:
        print(f"❌ PostgreSQL ჩაწერის შეცდომა: {e}")
        spark.stop()
        sys.exit(1)
