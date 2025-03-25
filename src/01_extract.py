from config import init_spark, load_spark
import requests

try:
    spark, sc = load_spark()
except FileNotFoundError:
    print("Initialisation d'une nouvelle session Spark...")
    spark, sc = init_spark()

# url = "https://data.lillemetropole.fr/data/ogcapi/collections/ilevia:vlille_temps_reel/items?f=json&limit=-1"

# response = requests.get(url)

# if response.status_code == 200:
#     data = response.json()
#     print("données Vlille DL")
# else:
#     print(f"erreur: {response.status_code}")
