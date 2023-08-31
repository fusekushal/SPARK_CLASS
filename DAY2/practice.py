from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, desc, concat

# Create a Spark session
spark = SparkSession.builder.appName("day2").getOrCreate()

# Load the Occupation dataset into a Spark DataFrame
data_path = "occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)

occupation.printSchema()

occupation.select("user_id", "age", "occupation").show()

occupation.select("*").filter(col("age")>30).show()

occupation.groupBy("occupation").count().show()

occupation.withColumn("age_group", \
   when((occupation.age >= 18) & (occupation.age <= 25), lit("18-25")) \
     .when((occupation.age >= 26) & (occupation.age <= 35), lit("26-35")) \
      .when((occupation.age >= 36) & (occupation.age <= 50), lit("36-50")) \
     .otherwise(lit("51+")) \
  ).show()

Sample_Data = [("James", " ","Smith", "36636", "M", 3000),	
          ("Micheal", "Rose", " ", "40288", "M", 4000),
          ("Robert", " ","Williams", "42114", "M", 4000),
          ("Maria", "Anne","Jones", "39192", "F", 4000),
          ("Jen", "Mary", "Brown", " ", "F", -1)
  ]	

Sample_schema = ["firstname","middlename","lastname","id","gender","salary"]	
dataframe = spark.createDataFrame(data = Sample_Data, schema = Sample_schema)	

dataframe.printSchema()	
dataframe.show(truncate=False)	

df = occupation.withColumn("gender",lit("Unknown")) # adds a new column gender but since it already has it simply overwrites 
df2 = df.withColumnRenamed("age","Years") #renames the column age to years
df2.show()

df3 = df2.select("*").filter(col("Years")>30)
df3.sort(desc("Years")).show()

partitioned_data = dataframe.coalesce(2)
rows = partitioned_data.collect() #for smaller datasets, use this. It stores in array format
for a in rows:
    print(a)

number_of_partition = partitioned_data.rdd.getNumPartitions()
print("Number of Partitions =",number_of_partition)

# Spark SQL
occupation.createOrReplaceTempView("occu_temp")

result = spark.sql("""select user_id, 
age, 
gender as sex,
occupation, 
zip_code,
case when age > 30 then True else False end as is_elderly
from occu_temp""")
result.show()

# Pyspark
occupation1 = occupation.withColumnRenamed("gender", "sex")
occupation1.withColumn("is_elderly", \
   when((occupation.age > 30), lit("true")) \
     .otherwise(lit("false")) \
  ).show()


# Spark SQL
occupation.createOrReplaceTempView("occu_temp1")
resul = spark.sql("""select gender, 
avg(age) as avg_age
from occu_temp1
group by gender""")
resul.show()

# Pyspark
from pyspark.sql.functions import avg
occuu1 = occupation.groupBy("gender").agg(avg("age").alias("avg_age"))
occuu1.show()

# Spark SQL
occupation.createOrReplaceTempView("occu_temp2")

result1 = spark.sql("""select user_id, 
age, 
gender,
occupation, 
zip_code as postal_code,
concat(user_id, occupation) AS full_name
from occu_temp2""")

result1.show()

# Pyspark
#to be discussed
dff2=occupation.select(concat(occupation.user_id, occupation.occupation).alias("full_name"), "user_id", "age", "gender", "occupation", "zip_code")
dff2.show()

