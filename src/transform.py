from extract import init_or_load_spark, get_extracted_data, get_data_vlille
from pyspark.sql import functions as F

spark = init_or_load_spark()
records = get_data_vlille(spark)
df_spark = get_extracted_data(spark, records)

# df_spark.show(1)

# Transformation format date
df_spark = df_spark.withColumn('date', F.date_format(F.to_timestamp(df_spark['date'], 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'), 'yyyy-MM-dd HH:mm'))
df_spark = df_spark.orderBy(F.desc('date'))
df_spark.show(2)
