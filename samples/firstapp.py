from pyspark import SparkContext
# logFile = "file:///home/student/spark/README.md"  
logFile = "hdfs:///user/student/shakespeare/cleopatra.txt"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'the' in s).count()
numBs = logData.filter(lambda s: 'those' in s).count()
print ("Lines with the: %i, lines with those: %i" % (numAs, numBs))
