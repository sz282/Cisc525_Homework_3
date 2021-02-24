#!/usr/bin/python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys, subprocess

sc = SparkContext(appName="StreamingWC")
out_dir = '/tmp/stream-'
count_value = 1

'''
datastream type.
'''
class Data_Stream():
    def __init__(self):

        self.ssc = StreamingContext(sc, 1)
        
    def streaming(self, port_no):
        line = self.ssc.socketTextStream('localhost', port_no)
        counts = line\
            .flatMap(lambda line: strip_punc(line))\
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
       
        counts.pprint(1000)       
        self.ssc.start()
        self.ssc.awaitTermination()

def get_count_value():
    return count_value

def inc_count_value():
    count_value = get_count_value() + 1
    
def delete_out_dir(out_dir):
    subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])           
        
def _print_each(rdd):
    print (out_dir, get_count_value())
    if rdd.count() > 1:
        newdir = out_dir + str(get_count_value())
        delete_out_dir(newdir)
        rdd.saveAsTextFile(newdir)
        inc_count_value()
        
def strip_punc(s):
    # return s.translate(str.maketrans(' ', ' ', string.punctuation)).split(' ')
    return s.split(' ')
       
def main(argv):
    data_stream = Data_Stream()
    data_stream.streaming(int(argv[0]))

if __name__ == '__main__':
    main(sys.argv[1:])        