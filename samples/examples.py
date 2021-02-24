# Need to run 'sc' first
from pyspark import SparkContext, SparkConf, SQLContext
conf = SparkConf().setAppName('sample_app')
sc=SparkContext(conf=conf)

from pyspark.sql.types import Row
from datetime import datetime

simple_data = sc.parallelize([1, "Kiet", 100])
simple_data.count()
simple_data.first()
simple_data.take(2)
simple_data.collect()

# Another example ...
airport = sc.parallelize(["00M","Thigpen ","Bay Springs","MS","USA","31.95376472","-89.23450472"])
airport.count()
airport.take(3)
airport.collect()
# expect an error because airport is NOT a DataFrame 
airportDF = airport.toDF()


airport = sc.parallelize([Row(iata="00M",airport="Thigpen ",city="Bay Springs",state="MS",country="USA",lat=31.95376472,long=-89.23450472)])
airport.collect()
airport.toDF().show()

airports = sc.parallelize([["00M","Thigpen ","Bay Springs","MS","USA",31.95376472,-89.23450472],
                         ["00R","Livingston Municipal","Livingston","TX","USA",30.68586111,-95.01792778]])
airports.toDF().show()

# Complext data type
complex = sc.parallelize([Row(col_float=3.1415,
                              col_string='da pi',
                              col_boolean=True,
                              col_integer=201,
                              col_list=[1,2,3,4])])
complex.toDF().show()

"""
+-----------+---------+-----------+------------+----------+
|col_boolean|col_float|col_integer|    col_list|col_string|
+-----------+---------+-----------+------------+----------+
|       true|   3.1415|        201|[1, 2, 3, 4]|     da pi|
+-----------+---------+-----------+------------+----------+
"""

# another complex data type ...
realComplex = sc.parallelize([
    Row(col_list=[1,2,3], col_dict = {"pi": 3.1415}, col_row = Row(number=3, fraction=1415), col_time=datetime(2019,7,22,5,51,0)),
    Row(col_list=[3,4,5], col_dict = {"sqrt2": 1.4142}, col_row = Row(number=1, fraction=4142), col_time=datetime(2019,7,22,5,54,0)),
    Row(col_list=[6,7,9,10], col_dict = {"sqrt3": 1.73205}, col_row = Row(number=1, fraction=73205), col_time=datetime(2019,7,22,5,55,0))
])
realComplex.toDF().show()

""" output looks like this ...
+------------------+-------------+----------+-------------------+
|          col_dict|     col_list|   col_row|           col_time|
+------------------+-------------+----------+-------------------+
|    [pi -> 3.1415]|    [1, 2, 3]| [1415, 3]|2019-07-22 05:51:00|
| [sqrt2 -> 1.4142]|    [3, 4, 5]| [4142, 1]|2019-07-22 05:54:00|
|[sqrt3 -> 1.73205]|[6, 7, 9, 10]|[73205, 1]|2019-07-22 05:55:00|
+------------------+-------------+----------+-------------------+
"""

### Another example ...

airport = sc.parallelize(["00M","Thigpen ","Bay Springs","MS","USA",31.95376472,-89.23450472])
airport.collect()

airport = sc.parallelize([Row(iata="00M",airport="Thigpen ",city="Bay Springs",state="MS",country="USA",lat=31.95376472,long=-89.23450472)])
airport.toDF().show()

airports = sc.parallelize([["00M","Thigpen ","Bay Springs","MS","USA",31.95376472,-89.23450472],
                         ["00R","Livingston Municipal","Livingston","TX","USA",30.68586111,-95.01792778]])
airports.toDF().show()

""" Output
+---+--------------------+-----------+---+---+-----------+------------+
| _1|                  _2|         _3| _4| _5|         _6|          _7|
+---+--------------------+-----------+---+---+-----------+------------+
|00M|            Thigpen |Bay Springs| MS|USA|31.95376472|-89.23450472|
|00R|Livingston Municipal| Livingston| TX|USA|30.68586111|-95.01792778|
+---+--------------------+-----------+---+---+-----------+------------+
"""

# Use SQLContext
sqlContext = SQLContext(sc)
sqlContext

sqlContext.createDataFrame(airports).show()
""" Output
+---+--------------------+-----------+---+---+-----------+------------+
| _1|                  _2|         _3| _4| _5|         _6|          _7|
+---+--------------------+-----------+---+---+-----------+------------+
|00M|            Thigpen |Bay Springs| MS|USA|31.95376472|-89.23450472|
|00R|Livingston Municipal| Livingston| TX|USA|30.68586111|-95.01792778|
+---+--------------------+-----------+---+---+-----------+------------+
"""

sqlContext.createDataFrame(airports, ["iata","airport","city","state","country","lat","long"]).show()

""" Output
+----+--------------------+-----------+-----+-------+-----------+------------+
|iata|             airport|       city|state|country|        lat|        long|
+----+--------------------+-----------+-----+-------+-----------+------------+
| 00M|            Thigpen |Bay Springs|   MS|    USA|31.95376472|-89.23450472|
| 00R|Livingston Municipal| Livingston|   TX|    USA|30.68586111|-95.01792778|
+----+--------------------+-----------+-----+-------+-----------+------------+
"""

# Example #3
simple_data = sc.parallelize([Row(1, 'Kiet', 100), Row(2, 'John', 100), Row(3, 'Keith', 99)])
sqlContext.createDataFrame(simple_data).show()

"""
+---+-----+---+
| _1|   _2| _3|
+---+-----+---+
|  1| Kiet|100|
|  2| John|100|
|  3|Keith| 99|
+---+-----+---+
"""
col_names = Row('id', 'name', 'score')
new_simple_data = simple_data.map(lambda r: col_names(*r))
new_simple_data.collect()

"""
[Row(id=1, name='Kiet', score=100),
 Row(id=2, name='John', score=100),
 Row(id=3, name='Keith', score=99)]
"""

sqlContext.createDataFrame(new_simple_data).show()

"""
+---+-----+-----+
| id| name|score|
+---+-----+-----+
|  1| Kiet|  100|
|  2| John|  100|
|  3|Keith|   99|
+---+-----+-----+
"""

# Accessing data
realComplex = sc.parallelize([
    Row(col_list=[1,2,3], col_dict = {"pi": 3.1415}, col_row = Row(number=3, fraction=1415), col_time=datetime(2019,7,22,5,51,0)),
    Row(col_list=[3,4,5], col_dict = {"sqrt2": 1.4142}, col_row = Row(number=1, fraction=4142), col_time=datetime(2019,7,22,5,54,0)),
    Row(col_list=[6,7,9,10], col_dict = {"sqrt3": 1.73205}, col_row = Row(number=1, fraction=73205), col_time=datetime(2019,7,22,5,55,0))
])
realComplex.toDF().show()

"""
+------------------+-------------+----------+-------------------+
|          col_dict|     col_list|   col_row|           col_time|
+------------------+-------------+----------+-------------------+
|    [pi -> 3.1415]|    [1, 2, 3]| [1415, 3]|2019-07-22 05:51:00|
| [sqrt2 -> 1.4142]|    [3, 4, 5]| [4142, 1]|2019-07-22 05:54:00|
|[sqrt3 -> 1.73205]|[6, 7, 9, 10]|[73205, 1]|2019-07-22 05:55:00|
+------------------+-------------+----------+-------------------+

"""
realComplex.collect()[0][3]
"""
datetime.datetime(2019, 7, 22, 5, 51)
"""

cell_list = realComplex.collect()[0][1]
cell_list.append(150)
cell_list
"""cell_list
[1, 2, 3, 150]
"""
# NO CHANGE in the real thing ...
realComplex.collect()[0][1]

# Accessing RDD from DF
realComplexDF = realComplex.toDF()
realComplexDF.rdd.map(lambda x: (x.col_dict, x.col_row)).collect()

""" Output
[({'pi': 3.1415}, Row(fraction=1415, number=3)),
 ({'sqrt2': 1.4142}, Row(fraction=4142, number=1)),
 ({'sqrt3': 1.73205}, Row(fraction=73205, number=1))]
"""

realComplexDF.select('col_dict', 'col_row').show()
"""
+------------------+----------+
|          col_dict|   col_row|
+------------------+----------+
|    [pi -> 3.1415]| [1415, 3]|
| [sqrt2 -> 1.4142]| [4142, 1]|
|[sqrt3 -> 1.73205]|[73205, 1]|
+------------------+----------+
"""


##### DATASET #####
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark DataSet example").config("spark.config.option", "value").getOrCreate()
airports_ds = spark.read.option("header", "true").csv("/user/student/airline/airports.csv")
airports_ds.show()
airports_ds.count()
airports_ds.take(2)
airports_ds.first()
airports_ds.collect()

