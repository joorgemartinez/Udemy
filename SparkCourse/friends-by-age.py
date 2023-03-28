# Import the necessary libraries
from pyspark import SparkConf, SparkContext

# Set up the Spark configuration with a master URL of "local" and an application name of "FriendsByAge"
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")

# Create a new Spark context using the configuration settings
sc = SparkContext(conf=conf)

# Define a function to parse each line of the CSV file
def parseLine(line):
    # Split the line by comma and get the third and fourth values
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    # Return a tuple of age and number of friends
    return (age, numFriends)

# Load the CSV file into an RDD
lines = sc.textFile("Datasets/fakefriends.csv")

# Apply the parseLine function to each line in the RDD
rdd = lines.map(parseLine)

# Map each age to a tuple containing the number of friends and a count of 1
totalsByAge = rdd.mapValues(lambda x: (x, 1))

# Reduce by key to sum the total number of friends and the count of friends for each age
totalsByAge = totalsByAge.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Map each age to the average number of friends
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect the results into a list and print them out
results = averagesByAge.collect()
for result in results:
    print(result)

