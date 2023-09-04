from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()

# Load the Chipotle dataset into a Spark DataFrame
data_path = "US_Crime_Rates_1960_2014.csv"  # Replace with the actual path
US_Crime_Rates_1960_2014_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = "titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)

US_Crime_Rates_1960_2014_df.printSchema()
US_Crime_Rates_1960_2014_df.show()

print("Total number of records:", US_Crime_Rates_1960_2014_df.count())

from pyspark.sql.functions import countDistinct
#countDistinct returns unique and distinct count of the column and collect retrieves the result as a list of rows
CD = US_Crime_Rates_1960_2014_df.select(countDistinct("Year")).collect()[0][0] #[0][0] gets the count value from the first row of the first column
print("Number of distinct years:", CD)

from pyspark.sql.functions import approx_count_distinct
ACD = US_Crime_Rates_1960_2014_df.select(approx_count_distinct("Total", 0.01)).collect()[0][0] 
print("Approximate distinct values in 'Total' column:", ACD)

from pyspark.sql.functions import first, last
#the functions work based on their obvious names. They are based on the rows in DF not on the values
begg = US_Crime_Rates_1960_2014_df.select(first("Year")).collect()[0][0]
endd = US_Crime_Rates_1960_2014_df.select(last("Year")).collect()[0][0]
print("First year:", begg, "\nLast year:", endd)

from pyspark.sql.functions import min, max
#does the task as the name suggests for a given column
minn = US_Crime_Rates_1960_2014_df.select(min("Population")).collect()[0][0]
maxx = US_Crime_Rates_1960_2014_df.select(max("Population")).collect()[0][0]
print("Minimum population:", minn, "\nMaximum population:", maxx)

from pyspark.sql.functions import sumDistinct
sd = US_Crime_Rates_1960_2014_df.groupBy("Year").agg(sumDistinct("Property").alias("SumDistinctProperty"))
sd.show()

from pyspark.sql.functions import avg
av = US_Crime_Rates_1960_2014_df.select(avg("Murder")).collect()[0][0]
print("Average murder rate:", av)

from pyspark.sql.functions import struct
US_Crime_Rates_1960_2014_df.select("Year", struct("Violent", "Property").alias("CrimeSums")).show()

from pyspark.sql.functions import col, avg
tot = US_Crime_Rates_1960_2014_df.withColumn("AVG_Crime", (col("Violent")+ col("Property") + col("Murder") + col("Forcible_Rape") + col("Robbery") + col("Aggravated_assault") + col("Burglary")+ col("Larceny_Theft") + col("Vehicle_Theft")))
av = tot.agg(avg("AVG_Crime"))
print("Average of all crimes:", av.collect()[0][0])
tot.select("Year", "AVG_Crime").show()

from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = Window.orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cum_sum = US_Crime_Rates_1960_2014_df.withColumn("CumulativePropertySum", sum("Property").over(window_spec))
cum_sum.show()

pivoted = US_Crime_Rates_1960_2014_df.groupBy("Year").pivot("Year").sum("Robbery")
sorted_data_df = pivoted.orderBy("Year")
sorted_data_df.show()
