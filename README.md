# Projet ETL avec Spark
 
- Connexion à l’API dataMEL afin d’obtenir des informations sur les bornes V’Lille
- Extraction des données dans un dataframe *(date, etat, etat_connexion, id, nb_places_dispo, nb_velos_dispo, nom, x, y...)*
- Transformation des données avec Pyspark (formatage date, ajout colonne nb_places_total et colonne % remplissage, transformation de l’ID et séparation dans un df_reforme des bornes en état réformé
- Chargement des données sur Bigquery
