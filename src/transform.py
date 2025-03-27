from extract import init_or_load_spark
from config import RAW_DATA_PATH, REFORMED_STATIONS_PATH, os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round

def load_raw_data(spark):
    if not os.path.exists(RAW_DATA_PATH):
        print("Fichier JSON introuvable")
        return None
    
    df_raw_data = spark.read.json(RAW_DATA_PATH)
    print('Chargement des données effectué')
    ## Info data
    df_raw_data.printSchema()
    # df_raw_data.show(2)
    # df_raw_data.describe().show()
    print(f"Colonnes: {df_raw_data.columns}")
    print(f"Nombre de lignes: {df_raw_data.count()}")
    print(f"Type des données: {df_raw_data.dtypes}")
    return df_raw_data

def clean_data(df_raw_data):
    if df_raw_data is None:
        print("Dataframe introuvable")
        return None

    ## Liste valeurs de etat et etat connexion
    df_raw_data.select('etat', 'etat_connexion').distinct().show()
    ## add df reformed_data et filtre sur le df raw_data
    df_reformed_data = df_raw_data.filter(col("etat") == "RÉFORMÉ")
    df_data_filtre = df_raw_data.filter(col("etat") != "RÉFORMÉ")
    print(f"Nombre de stations réformée: {df_reformed_data.count()}")
    df_reformed_data.show(2)
    df_data_filtre.select('etat', 'etat_connexion').distinct().show()
    ## Export reformed vers json
    df_reformed_data.write.mode("overwrite").json(REFORMED_STATIONS_PATH)
    ## Check doublons (id, nom)
    list_col = ['id', 'nom']
    for i in list_col:
        check_duplicates = df_data_filtre.groupBy(i).count().filter(F.col('count') > 1)
        print(f"doublons {i} : {check_duplicates.count()}")
        check_duplicates.show()

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_raw_data = load_raw_data(spark)
    if df_raw_data:
        clean_data(df_raw_data)