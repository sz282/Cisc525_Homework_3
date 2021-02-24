# Apache Spark project

CISC 525 Big Data Architecture class - homework 12 & 13


## to run

```bash
cd /home/student/cisc_525/apache-spark
./word_count.py hdfs://localhost:9000/user/student/shakespeare/tragedy/hamlet.txt hdfs://localhost:9000/tmp/hamlet_output beseech
```

or

```bash
cd /home/student/cisc_525/apache-spark
python3 ./word_count.py hdfs://localhost:9000/user/student/shakespeare/tragedy/hamlet.txt hdfs://localhost:9000/tmp/hamlet_output beseech
```

## start-hdfs.bash

```bash
#!/bin/bash
cd /usr/local/hadoop/sbin
rm -rf /tmp/hadoop-student
hdfs namenode -format
./start-all.sh
```

## prepare hdfs data

```bash
#!/bin/bash
hdfs dfs -mkdir -p /user/student/airline
hdfs dfs -mkdir -p /user/student/shakespeare
hdfs dfs -mkdir /tmp
hdfs dfs -copyFromLocal ./airline/* /user/student/airline
hdfs dfs -copyFromLocal ./shakespeare/tragedy /user/student/shakespeare
```

## to run

```bash
 ./word_count.py hdfs://localhost:9000/user/student/shakespeare/tragedy/hamlet.txt hdfs://localhost:9000/tmp/hamlet_output beseech
 
 ./performance.py hdfs://localhost:9000/user/student/airline/1987.csv hdfs://localhost:9000/tmp/output

```

