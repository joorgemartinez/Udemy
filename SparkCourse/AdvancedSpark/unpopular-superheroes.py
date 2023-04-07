from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession with an app name "LeastPopularSuperhero"
spark = SparkSession.builder.appName("LeastPopularSuperhero").getOrCreate()

# Define a schema for the names dataset, which has two fields: id and name
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Load the names dataset into a Spark DataFrame, using the defined schema
names = spark.read.schema(schema).option("sep", " ").csv("Datasets/Marvel-names.txt")

# Load the graph dataset into a Spark DataFrame, where each line represents a co-appearance
# of two superheroes in a comic book
lines = spark.read.text("Datasets/Marvel-graph.txt")

# Parse the graph dataset to create a new DataFrame with columns for superhero ID and their number of co-appearances
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))



minConnectionsCount = connections.agg(func.min("connections")).first()[0]
minConnections = connections.filter(func.col("connections") == minConnectionsCount)


minConnectionsWithName = minConnections.join(names, "id")

print(f"The following characters have only {str(minConnectionsCount)} co-appearances.")

minConnectionsWithName.select("name").show()