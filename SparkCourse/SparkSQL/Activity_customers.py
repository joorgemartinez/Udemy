# import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# create a new SparkSession
spark = SparkSession.builder.appName("ActivityCustomers").master("local[*]").getOrCreate()

# define the schema for the CSV file
schema = StructType([ \
                     StructField("cust_ID", IntegerType(), True), \
                     StructField("prod_ID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

# read the CSV file into a DataFrame using the defined schema
df = spark.read.schema(schema).csv("Datasets/customer-orders.csv")

# print the schema of the DataFrame
df.printSchema()

# select only the columns we're interested in (cust_ID and amount)
df_filtered = df.select("cust_ID", "amount")

# group the DataFrame by cust_ID and calculate the total amount spent by each customer, round the result to 2 decimal places and give it an alias
# sort the DataFrame by the total amount spent (ascending)
# show the result, along with the total number of rows in the original DataFrame
df_filtered.groupBy("cust_ID").agg(func.round(func.sum("amount"),2).alias("Total_by_Customer")).sort("Total_by_Customer").show(df_filtered.count())
