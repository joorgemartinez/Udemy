# Import the necessary libraries
from pyspark import SparkConf, SparkContext
import collections

# Set up the Spark configuration with a master URL of "local" and an application name of "RatingsHistogram"
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# Create a new Spark context using the configuration settings
sc = SparkContext(conf=conf)

# Load the data file into an RDD
lines = sc.textFile("ml-100k/u.data")

# Extract the ratings from the RDD by splitting each line by whitespace and selecting the third element
ratings = lines.map(lambda x: x.split()[2])

# Count the occurrences of each rating
result = ratings.countByValue()

# Sort the results by key using an OrderedDict
sortedResults = collections.OrderedDict(sorted(result.items()))

# Print out each rating and its count in ascending order
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

  