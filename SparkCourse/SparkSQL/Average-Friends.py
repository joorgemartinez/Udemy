# Import the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession
from pyspark.sql import Row 
from pyspark.sql import functions as func


# Create a new SparkSession with the name "SparkSQL", or get an existing one if available
spark = SparkSession.builder.appName("AverageFriends").getOrCreate()

# Load a CSV file called "fakefriends-header.csv" and infer the schema of the data
# (i.e., figure out the data types of each column based on the data itself)
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("Datasets/fakefriends-header.csv")

# We create a new Dataframe only with columns a "age" and "friends".
people_age_friends = people.select("age","friends")

# We Group By Age and then we compute the average number of friends.
people_age_friends.groupBy("age").avg("friends").show()

# We sort previous example 
people_age_friends.groupBy("age").avg("friends").sort("age").show()

# We format it showing only 2 decimals on the average number of friends
people_age_friends.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

# We add an alias for the column created with the average of friends per age
people_age_friends.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()


# Finish the SparkSession to free up resources. 
spark.stop()

