import re
from pyspark import SparkConf, SparkContext

# Define a function to normalize words in text
def normalizeWords(text):
    # Use regular expressions to split text into lowercase words
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# Set up Spark configuration and context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load text file from local file system
input = sc.textFile("Datasets/Book.txt")

# Normalize the text by splitting it into words and flattening the result
words = input.flatMap(normalizeWords)

# Count the frequency of each word in the text
wordCounts = words.countByValue()

# Iterate over the word count dictionary and print the results
for word, count in wordCounts.items():
    # Encode the word as ASCII and ignore non-ASCII characters
    cleanWord = word.encode('ascii', 'ignore')
    # Print the word and its count as a string
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))

