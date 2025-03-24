Extraction des données V'Lille avec Spark & BigQuery
Ce projet consiste à extraire les données des stations V'Lille (Vélib' Lillois) depuis l'API dataMEL, à les nettoyer avec Apache Spark, puis à les charger dans BigQuery pour une analyse ultérieure. L'orchestration se fait avec Airflow pour automatiser l'ensemble du pipeline.
 
- Connexion à l’API dataMEL afin d’obtenir des informations sur les bornes V’Lille
- Extraction des données dans un dataframe *(date, etat, etat_connexion, id, nb_places_dispo, nb_velos_dispo, nom, x, y...)*
- Transformation des données avec Pyspark (formatage date, ajout colonne nb_places_total et colonne % remplissage, transformation de l’ID et séparation dans un df_reforme des bornes en état réformé
- Chargement des données sur Bigquery
