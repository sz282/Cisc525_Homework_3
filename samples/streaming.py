# Run this under Jupyter ...
# make sure to run sc first.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Install netcat
# sudo apt-get install netcat
# run nc -lk 9999

# sc = SparkContext(appName="StreamingWC")
ssc = StreamingContext(sc, 1)
lines_stream = ssc.socketTextStream("localhost", 9999)
words = lines_stream.flatMap(lambda line: line.split(" "))
word_pairs = words.map(lambda word: (word, 1))
wordCount = word_pairs.reduceByKey(lambda x, y: x + y)
wordCount.pprint()
ssc.start()
ssc.awaitTermination()


## how about scala ....