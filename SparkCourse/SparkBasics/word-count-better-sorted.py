import re
from pyspark import SparkConf, SparkContext

# A function to normalize the words in the text
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# Create a Spark context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load the input file
input = sc.textFile("Datasets/Book.txt")

# Split the input file into individual words and normalize them using the 'normalizeWords' function
words = input.flatMap(normalizeWords)

# Count the number of times each word appears in the file using the 'reduceByKey' function
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Sort the words by their count and collect the results
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# Print out the results
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

