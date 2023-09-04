from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Load the Chipotle dataset into a Spark DataFrame
data_path = "titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = 'chipotle.csv' # Replace with the actual path
chipotle_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = 'kalimati_tarkari_dataset.csv' # Replace with the actual path
kalimati_df = spark.read.csv(data_path, header=True, inferSchema=True)

print(titanic_df.printSchema(),chipotle_df.printSchema(),kalimati_df.printSchema())


from pyspark.sql.functions import col 
#titanic_df.show()
#cast() is a function from Column class that is used to convert the column into the other datatype.
tttn = titanic_df.withColumn("Fare",col("Fare").cast("integer"))
tttn.printSchema()

from pyspark.sql.functions import lit, when
titanic_df.withColumn("IsAdult",\
                              when((titanic_df.Age>18), lit("true"))\
                              .otherwise(lit("false"))).select("PassengerId", "Name", "Age", "IsAdult").show()

from pyspark.sql.functions import mean
titanic_df.groupBy("Sex").agg(mean("Age").alias("AvgAge")).show()

from pyspark.sql.functions import regexp_extract
chicken_df = chipotle_df.filter(regexp_extract(col("item_name"), r'\bChicken\b', 0) != '')
chicken_df.show()

from pyspark.sql.functions import regexp_extract
chicken_dff = chipotle_df.filter(col("item_name").rlike(r'^Ch'))
chicken_dff.show()

from pyspark.sql.functions import col
cnt = titanic_df.filter(col("Age").isNull()).count()
print("Number of passengers with missing age:", cnt)

from pyspark.sql.functions import coalesce
chipotle_df.select("*", coalesce(col("item_name"), col("choice_description")).alias("OrderDetails")).show(5)

from pyspark.sql.functions import avg
average_value = titanic_df.select(avg("Age")).collect()[0][0]
titanic_nnull = titanic_df.fillna({"Age": average_value})
titanic_nnull.show()

titanic_df.drop("Cabin").show()


default = 30
titanic_df.fillna(default, subset=["Age"]).show()

from pyspark.sql.functions import col
titanic_df.withColumn("Sex",\
                       when(col("Sex") == "male", "M")\
                       .otherwise("F")).show()

from pyspark.sql.functions import struct
kalimati = kalimati_df.select("*", struct("Minimum", "Maximum").alias("PriceRange"))
kalimati.show()

from pyspark.sql.functions import split
kdf = kalimati_df.select("*", split(col("Commodity"), "   ").alias("CommodityList"))
kdf.show(truncate=False)

from pyspark.sql.functions import explode
exploded_df = kdf.select("SN", "Date", "Unit", "Minimum", "Maximum", "Average", explode("CommodityList").alias("Commodity"))
exploded_df.show(truncate=False)

from pyspark.sql.functions import create_map
kalimati_df.select("*", create_map(col("Commodity"), col("Average")).alias("PriceMap"))\
        .show(truncate=False)

kalimati_json = kalimati_df.toJSON().collect()
filename = "Kalimati.json"

with open(filename, "w") as f:
    for json_row in kalimati_json:
        f.write(json_row + "\n")

print("Data written to", filename)

df = spark.read.json(spark.sparkContext.parallelize(kalimati_json))
df.show()