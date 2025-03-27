from extract import init_or_load_spark
from config import RAW_DATA_PATH, os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round

def load_raw_data(spark):
    if not os.path.exists(RAW_DATA_PATH):
        print("Fichier JSON introuvable")
        return None
    
    df_raw_data = spark.read.json(RAW_DATA_PATH)
    print('Chargement des données effectué')
    # Info data
    df_raw_data.printSchema()
    # df_raw_data.show(2)
    # df_raw_data.describe().show()
    print(f"Colonnes: {df_raw_data.columns}")
    print(f"Nombre de colonnes: {df_raw_data.count()}")
    print(f"Type des données: {df_raw_data.dtypes}")
    return df_raw_data

def transform_data(df_raw_data):
    if df_raw_data is None:
        print("Dataframe introuvable")
        return None
    
    # 1. Data cleaning
    # Liste des valeurs de etat et etat connexion
    df_raw_data.select('etat', 'etat_connexion').distinct().show()

    

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_raw_data = load_raw_data(spark)
    if df_raw_data:
        transform_data(df_raw_data)