# Import the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession

# Create a new SparkSession with the name "SparkSQL", or get an existing one if available
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Load a CSV file called "fakefriends-header.csv" and infer the schema of the data
# (i.e., figure out the data types of each column based on the data itself)
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("Datasets/fakefriends-header.csv")

# Print the inferred schema of the "people" DataFrame
print("Here is our inferred schema:")
people.printSchema()

# Select only the "name" column from the "people" DataFrame and display it
print("Let's display the name column:")
people.select("name").show()

# Filter out any rows where the "age" column is greater than or equal to 21 and display the remaining rows
print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

# Group the rows in the "people" DataFrame by their "age" values and count the number of rows in each group, then display the result
print("Group by age")
people.groupBy("age").count().show()

# Create a new DataFrame that has the same "name" column as the "people" DataFrame, but with each person's "age" increased by 10, and display it
print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

# Stop the SparkSession to free up resources
spark.stop()
