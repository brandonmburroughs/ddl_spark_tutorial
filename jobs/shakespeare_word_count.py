## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from operator import add

## Module Constants
APP_NAME = "Shakespeare Word Count Example"

## Closure Functions
# Define tokenization function
def tokenize(text):
	return text.split()

## Main functionality
def main(sc):
	# Read in data
	text = sc.textFile("../data/shakespeare.txt")

	# Map text file to tokenization function
	words = text.flatMap(tokenize)

	# Emit word count tuples
	wc = words.map(lambda x: (x,1))

	# Reduce counts
	counts = wc.reduceByKey(add)
	counts.saveAsTextFile("../results/wc")

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
