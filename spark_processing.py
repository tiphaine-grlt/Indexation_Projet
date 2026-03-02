from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MeteoProcessing").getOrCreate()

df = spark.read.json("meteo_data.json")

# Regrouper tous les calculs dans un seul tableau
stats = df.groupBy("city").agg(
    F.avg("temperature_2m").alias("avg_temp"),
    F.min("temperature_2m").alias("min_temp"),
    F.max("temperature_2m").alias("max_temp"),
    F.sum("precipitation").alias("total_precipitation"),
    F.avg("wind_speed_10m").alias("avg_wind")
)

print("=== Statistiques globales ===")
stats.show()

# Export unique CSV
stats.coalesce(1).write.mode("overwrite").csv("output_stats", header=True)

spark.stop()
