// run spark-shell command

val dataset = Seq((1, "Kiet", 100), (2, "Keith", 99)).toDS()
dataset.show

val airport = sc.parallelize(Seq("00M","Thigpen ","Bay Springs","MS","USA","31.95376472","-89.23450472"))
airport.count()
airport.take(3)
airport.collect()
// expect an error because airport is NOT a DataFrame 
val airportDS = airport.toDS()
airportDS.show


case class Airport(iata: String, airport: String, city: String, state: String, country: String, lat: Double, longi: Double)
val airport = Seq(Airport("00M","Thigpen ","Bay Springs","MS","USA",31.95376472,-89.23450472))
airport.collect
airport.toDS.show

// Word Count example: 
// https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-datasets.html

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" ")).filter(_ != "").groupBy("value")
val countsDataset = groupedDataset.count()
countsDataset.show()

// Join datasets
case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
case class Department(id: Int, name: String)

case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

val employeeDataset = employeeDataSet1.union(employeeDataSet2)

def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
  val (total, count) = iterator.foldLeft(0.0, 0.0) {
      case ((total, count), x) => (total + x.salary, count + 1)
  }
  ResultSet(key._1, key._2, total/count)
}

val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner").map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name)).filter(record => record.age > 25).groupBy($"departmentId", $"departmentName").avg()

// val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
//                                           .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
//                                           .filter(record => record.age > 25)
//                                           .groupBy($"departmentId", $"departmentName")
//                                           .avg()

averageSalaryDataset.show()

// Convert a dataset to a dataframe
import org.apache.spark.sql.functions._

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val result = wordsDataset.flatMap(_.split(" ")).filter(_ != "").map(_.toLowerCase()).toDF().groupBy($"value").agg(count("*") as "numOccurances").orderBy($"numOccurances" desc)

// val result = wordsDataset
//               .flatMap(_.split(" "))               // Split on whitespace
//               .filter(_ != "")                     // Filter empty words
//               .map(_.toLowerCase())
//               .toDF()                              // Convert to DataFrame to perform aggregation / sorting
//               .groupBy($"value")                   // Count number of occurrences of each word
//               .agg(count("*") as "numOccurances")
//               .orderBy($"numOccurances" desc)      // Show most common words first
result.show()

// Loading data from csv from hadoop
// rm -rf /tmp/hadoop-student
// hdfs namenode -format
// start-all.sh
// hdfs dfs -mkdir /user
// hdfs dfs -mkdir /user/student
// hdfs dfs -mkdir /user/student/airline
// hdfs dfs -copyFromLocal /home/student/dev/airline/airports.csv /user/students/airline

val sqlContext = new org.apache.spark.sql.SQLContext(sc);
val airports = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/student/airline/airports.csv")

//   val airports = sqlContext.read.format("csv")
//   .option("header", "true")
//   .option("inferSchema", "true")
//   .load("/user/student/airline/airports.csv")


// Example word count ..
val words = Array("one", "two", "two", "three", "three", "three")
val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
val wordCountsWithReduce = wordPairsRDD.reduceByKey(_+_).collect()
val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum)).collect()

wordPairsRDD.foreach{ x => println ("x = " + x) }

val example_data = sc.parallelize(1 to 10)
example_data.collect()

// Example of reading data from text file and count words.
val hamletText = sc.textFile("/user/student/shakespeare/tragedy/hamlet.txt")
hamletText.collect()
val words = hamletText.flatMap(_.toLowerCase.split(" ")).filter(_ != "").toDS.groupBy("value")
words.count().foreach{x => println(


// Additional operations on RDD ....val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
// Narrow transformat
val example_data = sc.parallelize(1 to 10)
val example_data_by_2 = example_data.map(_*2)
example_data_by_2.collect

// flatMap
val flatWords = hamletText.flatMap(x => x.toLowerCase.split(" "))
flatWords.foreach{x => println(x)}
flatWords.filter(_ != "").foreach{x => println(x)}
flatWords.filter(_ != "").toDS.groupBy("value").count.foreach{x => println(x)}
hamletText.flatMap(x => x.toLowerCase.split(" ")).filter(_ != "").toDS.groupBy("value").count.foreach{x => println(x)}
// filter
val listA = words.count.filter(x => {x.get(0) == "cold" || x.get(0) == "conversation"})
val listB = words.count.filter(x => {x.get(0) == "rosencrantz" || x.get(0) == "polonius"})

// union
val listC = listA.union(listB)
listC.foreach{x => println(x)}

// distinct

flatWords.count
flatWords.distinct.count

// sortBy
val sorted = sc.parallelize(Array(("Kiet", 4),("Joe", 2),("Anna", 6),("Zebra", 1))).sortBy( _._2 * -1 )
val sortedList = sorted.collect.toList
sortedList(0)


// intersect
val listA = words.count.filter(x => {x.get(0) == "cold" || x.get(0) == "conversation"})
val listB = words.count.filter(x => {x.get(0) == "rosencrantz" || x.get(0) == "polonius" || x.get(0) == "conversation"})

// substract
val listC = words.count.filter(x => {x.get(0) == "polonius" || x.get(0) == "conversation"})
listB.rdd.subtract(listC.rdd).collect

// cartesian
val number = sc.parallelize(Array("A", "B", "C"))
val alpha = sc.parallelize(1 to 5)
number.cartesian(alpha).collect


