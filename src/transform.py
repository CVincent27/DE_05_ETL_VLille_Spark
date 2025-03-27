from extract import init_or_load_spark
from config import RAW_DATA_PATH, os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round

def load_raw_data(spark):
    if not os.path.exists(RAW_DATA_PATH):
        print("Fichier JSON introuvable")
        return None
    
    df_raw_data = spark.read.json(RAW_DATA_PATH)
    df_raw_data.printSchema()
    return df_raw_data

def transform_data(df_raw_data):
    if df_raw_data is None:
        print("Dataframe introuvable")
        return None

    # 1. Formatage de la data
    df_format_date = df_raw_data.withColumn('date', F.date_format(F.to_timestamp(df_raw_data['date'], 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'), 'yyyy-MM-dd HH:mm'))
    df_format_date = df_format_date.orderBy(F.desc('date'))
    # df_format_date.show(5)

    # 2. Add nb_places_totales et % full
    df_add_col = df_format_date.withColumn('nb_places_totales', df_format_date['nb_places_dispo'] + df_format_date['nb_velos_dispo'])
    df_add_col.show(5)

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_raw_data = load_raw_data(spark)
    if df_raw_data:
        transform_data(df_raw_data)