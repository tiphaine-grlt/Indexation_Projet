from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MeteoProcessing").getOrCreate()

# Charger JSON exporté
df = spark.read.json("meteo_data.json")

# Afficher schéma
df.printSchema()

# Moyenne température par ville
avg_temp = df.groupBy("city").avg("temperature_2m")
avg_temp.show()

# Total précipitations par ville
sum_prec = df.groupBy("city").sum("precipitation")
sum_prec.show()

# Température max
max_temp = df.groupBy("city").max("temperature_2m")
max_temp.show()

# Export CSV
avg_temp.write.mode("overwrite").csv("output_avg_temp", header=True)

spark.stop()
