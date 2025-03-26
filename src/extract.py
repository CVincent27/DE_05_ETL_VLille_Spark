from config import init_spark, load_spark
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
    } for record in records]    
    df_spark = spark.createDataFrame(extracted_data)
    df_spark.show(1)
    return df_spark

if __name__ == "__main__":
    spark = init_or_load_spark()
    df_spark = get_data_vlille(spark)
