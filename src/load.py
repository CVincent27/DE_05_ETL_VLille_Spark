from transform import init_or_load_spark, CLEAN_DATA_PATH
import os

def load_clean_data(spark):
    if not os.path.exists(CLEAN_DATA_PATH):
        print("Fichier JSON introuvable")
        return None

    clean_data = spark.read.json(CLEAN_DATA_PATH)
    clean_data.show(2)

if __name__ == "__main__":
    spark = init_or_load_spark()
    load_clean_data(spark)
