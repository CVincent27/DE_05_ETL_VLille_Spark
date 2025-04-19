from transform import init_or_load_spark, CLEAN_DATA_PATH
import os


def load_clean_data(spark):
    if not os.path.exists(CLEAN_DATA_PATH):
        print("Fichier JSON introuvable")
        return None

    clean_data = spark.read.json(CLEAN_DATA_PATH)
    clean_data.show(2)
    return clean_data

def write_to_mysql(df, jdbc_url, connection_properties, table_name):
    try:
        if df.count() > 0:
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode='overwrite', 
                properties=connection_properties
            )
            print(f"Données écrites dans la table {table_name}.")
        else:
            print("Le DataFrame est vide. Aucune donnée à écrire.")
    except Exception as e:
        print(f"Erreur lors de l'écriture dans MySQL : {e}")

if __name__ == "__main__":
    spark = init_or_load_spark()
    clean_data = load_clean_data(spark)

    # Configuration de la connexion MySQL
    jdbc_url = "jdbc:mysql://localhost:3306/de_spark_vlille"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver",
        "socketTimeout": "300000",  
        "connectTimeout": "300000", 
        "useSSL": "false"
    }

    table_name = "clean_data_vlille"

    if clean_data is not None:
        write_to_mysql(clean_data, jdbc_url, connection_properties, table_name)
    else:
        print("Aucune donnée à écrire dans MySQL.")
