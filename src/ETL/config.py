import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
import json

CONFIG_PATH = "./spark_config.json"
RAW_DATA_PATH = "./src/data/raw_data.json"
REFORMED_STATIONS_PATH = "./src/data/reformed_stations.json"
CLEAN_DATA_PATH = "./src/data/clean_data/clean_data.json"
CLEAN_DATA_CSV = "./src/data/clean_data/clean_data.csv"

SQL_CONNECTOR_PATH = r"C:\spark\mysql-connector-j-9.2.0\mysql-connector-j-9.2.0.jar"

def init_spark():
    spark = SparkSession.builder \
    .config("spark.jars", SQL_CONNECTOR_PATH) \
    .master("local[*]") \
    .getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")
    # Définir le niveau de log pour mieux voir les erreurs
    sc.setLogLevel("ERROR")

    # Sauvegarde de la config Spark
    config = spark.sparkContext.getConf().getAll()
    with open(CONFIG_PATH, "w") as f:
        json.dump(dict(config), f)

    print("Spark init")
    return spark, sc

def load_spark():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("execute `init_spark()`.")

    # load config spark
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    # recréer session avec la même config
    spark_builder = SparkSession.builder.master("local[*]")
    for k, v in config.items():
        spark_builder = spark_builder.config(k, v)

    spark = spark_builder.getOrCreate()
    sc = spark.sparkContext
    print("Spark chargé avec la config sauvegardée")
    return spark, sc
