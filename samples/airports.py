from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

import sys

class Airports():
    def __init__(self):
        self.conf = SparkConf().setAppName('airports_app')
        self.sc=SparkContext(conf=self.conf)
        
    def load_airports(self, airports):        
        spark = SparkSession.builder.appName("Airports").config("spark.config.option", "value").getOrCreate()
        self.df = spark.read.option("header", "true").csv(airports).select('*').toPandas()
        return self.df
        
def main(argv):
    airports = Airports()
    df = airports.load_airports(argv[0])
    
    for row in df.itertuples():
        buf = ''
        for col in range(1, len(row)):
            buf += row[col] + ', '
        print(buf)


if __name__ == '__main__':
    main(sys.argv[1:])