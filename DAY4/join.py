from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

departments_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department_name", StringType(), True)
])

employees_data = [
    (1, "Pallavi mam", 101),
    (2, "Bob", 102),
    (3, "Cathy", 101),
    (4, "David", 103),
    (5, "Amrit Sir", 104),
    (6, "Alice", None),
    (7, "Eva", None),
    (8, "Frank", 110),
    (9, "Grace", 109),
    (10, "Henry", None)
]

departments_data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Finance"),
    (104, "Marketing"),
    (105, "Operations"),
    (106, None),
    (107, "Operations"),
    (108, "Production"),
    (None, "Finance"),
    (110, "Research and Development")
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
departments_df = spark.createDataFrame(departments_data, schema=departments_schema)

employees_df.show()
employees_df.printSchema()
departments_df.show()
departments_df.printSchema()

#basic syntax of join is 'df1.join(df2, joinExpression, joinType)
#here joinexpression is the common column and jointype is the type of join to be performed
employees_df.join(departments_df, "department_id", "inner").show(truncate = False)

ijj = employees_df.join(departments_df, "department_id", "inner")
ij = ijj.select("employee_name", "department_name")
ij.where(ij.department_name == "Engineering").show()

ojj = employees_df.join(departments_df, "department_id", "outer")
oj = ojj.select("employee_name", "department_name")
#oj.show()
filled_oj = oj.fillna("No Department", subset=["department_name"])
filled_oj.show() #according to question
actual_oj = oj.fillna("No Department")
actual_oj.show() #according to expected output

lojj = employees_df.join(departments_df, "department_id", "left_outer")
loj = lojj.select("employee_name", "department_name")
loj.fillna("No Department").show()

rojj = employees_df.join(departments_df, "department_id", "right_outer")
roj = rojj.select("department_name", "employee_name")
roj.fillna("No Employee").show()

lsjj = employees_df.join(departments_df, "department_id", "left_semi")
lsj = lsjj.select("employee_name")
lsj.show()
lsjj.show()

lajj = employees_df.join(departments_df, "department_id", "left_anti")
laj = lajj.select("employee_name")
laj.show()

#employees_df.join(departments_df, "department_id", "cross")
employees_df.crossJoin(departments_df).show()