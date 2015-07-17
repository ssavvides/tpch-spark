# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API (introduced in Spark 1.3.0)

Savvas Savvides

ssavvides@us.ibm.com

savvas@purdue.edu

### Running

First compile using:

```
sbt package
```

You can then run a query using:

```
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.10/spark-tpc-h-queries_2.10-1.0.jar ##
```

where ## is the number of the query to run e.g 1, 2, ..., 22

### Other Implementations

1. Data generator (http://www.tpc.org/tpch/)

2. TPC-H for Hive (https://issues.apache.org/jira/browse/hive-600)

3. TPC-H for PIG (https://github.com/ssavvides/tpch-pig)
