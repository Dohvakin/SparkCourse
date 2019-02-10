import collections  # collection for basic operations on the dataset like creating dictionaries

from pyspark import SparkConf, SparkContext  # imports from spark, SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# The spark is to be run locally without creating a distributed cluster computation
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])  # lambda is the values by ' ' and then extracting [2] item from the list
result = ratings.countByValue()  # counts the no of repeated no. and transforms it into (1,6110)

# noinspection PyTypeChecker
sortedResults = collections.OrderedDict(sorted(result.items()))  # Ratings are sorted into a dictionary
for key, value in sortedResults.items():  # For evert value of (key, value) print them to the console
    print("%s %i" % (key, value))  # Printing the values of the Dictionary
# TODO Analise the code again later to get a through understanding
