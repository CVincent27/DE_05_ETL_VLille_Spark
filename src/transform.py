from extract import init_or_load_spark
from config import RAW_DATA_PATH, os
from pyspark.sql import functions as F

def load_raw_data(spark):
    if not os.path.exists(RAW_DATA_PATH):
        print("Fichier JSON introuvable")
        return None
    
    df_raw_data = spark.read.json(RAW_DATA_PATH)
    df_raw_data.printSchema()
    return df_raw_data

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_raw_data = load_raw_data(spark)
