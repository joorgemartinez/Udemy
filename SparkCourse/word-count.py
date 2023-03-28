# Import the necessary libraries
from pyspark import SparkConf, SparkContext

# Set up the Spark configuration with a master URL of "local" and an application name of "WordCount"
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load the input file into an RDD
input = sc.textFile("Datasets/Book.txt")

# Split each line into words and create a new RDD of (word, 1) pairs. The method split(), will segment the words by the blank space.
words = input.flatMap(lambda x: x.split())

# Count the number of occurrences of each word
wordCounts = words.countByValue()

# Iterate over the wordCounts dictionary and print out each word and its count
for word, count in wordCounts.items():
    # Clean up the word by removing any non-ASCII characters
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        # Print the word and its count
        print(cleanWord.decode() + " " + str(count))
