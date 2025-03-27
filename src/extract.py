from config import init_spark, load_spark, RAW_DATA_PATH, os
import requests

def init_or_load_spark():
    try:
        spark, sc = load_spark()
    except FileNotFoundError:
        print("Init nouvelle session Spark")
        spark, sc = init_spark()
    return spark

def get_data_vlille(spark):
    url = "https://data.lillemetropole.fr/data/ogcapi/collections/ilevia:vlille_temps_reel/items?f=json&limit=-1"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print("données V'lille DL")
    else:
        print(f"erreur: {response.status_code}")
        return  

    # print(type(data))
    records = data.get("records", [])
    # print(type(records))
    print(records[:1])
    return records

def get_extracted_data(spark, records, limit=5):
    if not records:
        print("Aucune donnée")
        return None
    
    data_dir = os.path.dirname(RAW_DATA_PATH)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    extracted_data = [{
    'id': record.get('@id', None),
    'nom': record.get('nom', None),
    'etat' : record.get('etat', None),
    'nb_places_dispo' : record.get('nb_places_dispo', None),
    'nb_velos_dispo' : record.get('nb_velos_dispo', None),
    'etat_connexion' : record.get('etat_connexion', None),
    'x' : record.get('x', None),
    'y' : record.get('y', None),
    'date' : record.get('date_modification', None),
    } for record in records[:limit]]

    df_spark = spark.createDataFrame(extracted_data)
    df_spark.write.mode("overwrite").json(RAW_DATA_PATH)
    # df_spark.show(1)
    print(f"dataframe : {df_spark} crée et sauvegardé ici : {RAW_DATA_PATH}")
    df_spark.select("id").show(1)
    return df_spark

if __name__ == "__main__":
    spark = init_or_load_spark()
    records = get_data_vlille(spark)
    if records:
        df_spark = get_extracted_data(spark, records)
