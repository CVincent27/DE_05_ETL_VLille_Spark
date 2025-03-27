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
    # df_raw_data.printSchema()
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
    # df_reformed_data.show(2)
    df_data_filtre.select('etat', 'etat_connexion').distinct().show()
    ## Export reformed vers json
    df_reformed_data.write.mode("overwrite").json(REFORMED_STATIONS_PATH)
    ## Check doublons (id, nom)
    list_col = ['id', 'nom']
    for i in list_col:
        check_duplicates = df_data_filtre.groupBy(i).count().filter(F.col('count') > 1)
        print(f"doublons {i} : {check_duplicates.count()}")
    return df_data_filtre

def format_date(df_data_filtre):
    df_format_date = df_data_filtre.withColumn('date', F.date_format(F.to_timestamp(df_data_filtre['date'], 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'), 'yyyy-MM-dd HH:mm'))
    df_format_date = df_format_date.orderBy(F.desc('date'))
    #df_format_date.show(1)
    return df_format_date

# Ajout de col nb_places_total et %_full
def add_col_infos(df_format_date):
    df_data_add_col = df_format_date.withColumn('nb_places_totales', df_format_date['nb_places_dispo'] + df_format_date['nb_velos_dispo'])
    df_data_add_col = df_data_add_col.withColumn('%_full',((col('nb_velos_dispo')) / col('nb_places_totales')) * 100)
    df_data_add_col = df_data_add_col.withColumn('%_full', round(col('%_full'), 2))
    df_data_add_col.show(3)
    print(df_data_add_col.filter(col('%_full').isNull()).count())
    return df_data_add_col

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_raw_data = load_raw_data(spark)
    if df_raw_data:
        df_data_filtre = clean_data(df_raw_data)
        df_format_date = format_date(df_data_filtre)
        add_col_infos(df_format_date)