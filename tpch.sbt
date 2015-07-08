// to generate jar:
// sbt package

// to run on standalone:
// spark-submit --class "CLASSNAME" --master spark://savvass-mbp.watson.ibm.com:7077 target/scala-2.10/JARNAME

name := "Spark Examples Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.4.0"