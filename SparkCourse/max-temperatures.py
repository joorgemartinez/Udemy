# Import the necessary PySpark modules
from pyspark import SparkConf, SparkContext

# Set the Spark configuration and context
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# Define a function to parse each line of the CSV file
def parseLine(line):
    fields = line.split(',') # Split the line into fields
    stationID = fields[0]   # Extract the station ID from the first field
    entryType = fields[2]   # Extract the entry type from the third field
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0  # Convert the temperature to Fahrenheit
    return (stationID, entryType, temperature)  # Return a tuple with the station ID, entry type, and temperature

# Load the CSV file as an RDD of strings
lines = sc.textFile("Datasets/1800.csv")

# Parse each line using the parseLine function and convert the resulting RDD to a new RDD
parsedLines = lines.map(parseLine)

# Filter the RDD to keep only the entries with "TMAX" as the entry type
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# Map the station ID and temperature from the maxTemps RDD to a new RDD
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# Use the reduceByKey method to find the maximum temperature for each station
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

# Collect the results into a list and loop through them to print the station ID and minimum temperature to the console
results = maxTemps.collect();
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))