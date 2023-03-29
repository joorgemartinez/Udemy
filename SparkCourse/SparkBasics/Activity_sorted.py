from pyspark import SparkConf, SparkContext

# Set up the Spark configuration with a master URL of "local" and an application name of "FriendsByAge"
conf = SparkConf().setMaster("local").setAppName("AmountByCostumer")

# Create a new Spark context using the configuration settings
sc = SparkContext(conf=conf)

# Define a function to parse each line of the CSV file
def extractCustomersPairs(line):
    # Split the line by comma and get the firsr and third values
    fields = line.split(',')
    age = int(fields[0])
    numFriends = float(fields[2])
    # Return a tuple of age and number of friends
    return (age, numFriends)

# Load the CSV file into an RDD
lines = sc.textFile("Datasets/customer-orders.csv")

# Apply the extractCustomersPairs function to each line in the RDD
rdd = lines.map(extractCustomersPairs)

# Use ReduceByKey to add the values of the amounts per each ID
amount_by_customer = rdd.reduceByKey((lambda x, y: x + y))

# We switch key and value to have the amount as the key and then sort them in descending order
amount_by_costumer_sorted = amount_by_customer.map(lambda x: (x[1], x[0])).sortByKey(False)

#Print the results with a format of 2 decimals
results = amount_by_costumer_sorted.collect()
for result in results:
    print("({:.2f},{})".format(result[0], result[1]))
