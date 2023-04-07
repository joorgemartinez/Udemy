from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession with an app name "MostPopularSuperhero"
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# Define a schema for the names dataset, which has two fields: id and name
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Load the names dataset into a Spark DataFrame, using the defined schema
names = spark.read.schema(schema).option("sep", " ").csv("Datasets/Marvel-names.txt")

# Load the graph dataset into a Spark DataFrame, where each line represents a co-appearance
# of two superheroes in a comic book
lines = spark.read.text("Datasets/Marvel-graph.txt")

# Parse the graph dataset to create a new DataFrame with columns for superhero ID and their
# number of co-appearances
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Find the superhero with the most co-appearances by sorting the connections DataFrame in
# descending order and selecting the first row
mostPopular = connections.sort(func.col("connections").desc()).first()

# Find the name of the most popular superhero by joining the names DataFrame with the
# connections DataFrame on the superhero ID, filtering for the most popular superhero ID,
# selecting their name, and getting the first row
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# Print the name of the most popular superhero and their number of co-appearances
print(f"The most popular superhero is {mostPopularName[0]} with {str(mostPopular[1])} co-appearances.")
