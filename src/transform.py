from extract import init_or_load_spark, get_extracted_data, get_data_vlille
from pyspark.sql import functions as F

spark = init_or_load_spark()
records = get_data_vlille(spark)
df_spark = get_extracted_data(spark, records)

df_spark.show(2)
