# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("LeastPopularSuperhero").getOrCreate()

# Define the schema for the Marvel names dataset
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Load the Marvel names dataset into a DataFrame using the defined schema
names = spark.read.schema(schema).option("sep", " ").csv("Datasets/Marvel-names.txt")

# Load the Marvel graph dataset into a DataFrame
lines = spark.read.text("Datasets/Marvel-graph.txt")

# Split the values in the Marvel graph DataFrame by spaces and extract the superhero ID and number of connections
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Find the minimum number of connections and filter for superheroes with that number of connections
minConnectionsCount = connections.agg(func.min("connections")).first()[0]
minConnections = connections.filter(func.col("connections") == minConnectionsCount)

# Join the DataFrame of superheroes with the minimum number of connections with the names DataFrame to get their names
minConnectionsWithName = minConnections.join(names, "id")

# Print the result
print(f"The following characters have only {str(minConnectionsCount)} co-appearances.")
minConnectionsWithName.select("name").show()
