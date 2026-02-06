from pyspark.sql import SparkSession
import sys

def get_spark():
    try:
        spark = (
            SparkSession.builder
            .appName("MSSQL to PostgreSQL Bronze ETL")
            .config("spark.jars.packages",
                    "com.microsoft.sqlserver:mssql-jdbc:13.2.1.jre11,org.postgresql:postgresql:42.7.4")
            .config("spark.sql.adaptive.enabled", "true")
            # აიღებს WARN-ს მხოლოდ კონკრეტულ დონეზე
            .config("spark.sql.debug.maxToStringFields", 1000)
            .getOrCreate()
        )
        # log level-ი production friendly
        spark.sparkContext.setLogLevel("ERROR")  # ან "WARN" თუ გინდა მხოლოდ სხვა WARN-ები დაინახო
        print("✅ Spark Session Has been Created!")
        return spark
    except Exception as e:
        print(f"❌ Spark Error: {e}")
        sys.exit(1)
