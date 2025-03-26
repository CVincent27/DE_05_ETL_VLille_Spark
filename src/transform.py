from extract import init_or_load_spark, get_data_vlille

spark = init_or_load_spark()
df_spark = get_data_vlille(spark)

df_spark.show(5)

