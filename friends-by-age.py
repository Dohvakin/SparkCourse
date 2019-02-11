from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numfriends = int(fields[3])
    return age, numfriends


lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
# call the function parseline
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# add the no. of friends of X[0] with second y[0] and add no. of people of that age x[1] and y[1]
averagesByAge = totalsByAge.mapValues(lambda x: x[0] // x[1])
# divide the no. of friends with the no. off people of that age
results = sorted(averagesByAge.collect())  # sort the values for give age
for result in results:
    print(result)
