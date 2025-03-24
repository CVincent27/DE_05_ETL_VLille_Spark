import findspark
findspark.init()

from pyspark.sql import SparkSession
import json
import os

CONFIG_PATH = "/content/spark_config.json"

def init_spark():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Sauvegarde de la configuration Spark
    config = spark.sparkContext.getConf().getAll()
    with open(CONFIG_PATH, "w") as f:
        json.dump(dict(config), f)

    print("Spark init")
    return spark, sc

def load_spark():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("Lancer d'abord `init_spark()`.")

    # Charger config
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    # Recréer session spark avec la même config
    spark_builder = SparkSession.builder.master("local[*]")
    for k, v in config.items():
        spark_builder = spark_builder.config(k, v)

    spark = spark_builder.getOrCreate()
    sc = spark.sparkContext
    print("Spark chargé avec la config sauvegardée")
    return spark, sc
