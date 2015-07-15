# tpch-spark
TPC-H queries in spark

Savvas Savvides

ssavvides@us.ibm.com

savvas@purdue.edu

Data generator from:

=> TPC-H DBGEN (http://www.tpc.org/tpch/)

### Running

First compile using:

```
sbt package
```

You can then run a query using:

```
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.10/spark-examples-project_2.10-1.0.jar ##
```

where ## is the number of the query to run e.g 1, 2, ..., 22

