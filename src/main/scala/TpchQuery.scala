package main.scala

import java.io.File
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {
  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  // get the name of the class excluding dollar signs and package
  def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  /**
    * Execute query reflectively
    */
  def executeQuery(queryNo: Int, spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider, outputDir: String): Long = {
    assert(queryNo >= 1, "Invalid query number")
    val t0 = System.nanoTime()
    val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
    outputDF(query.execute(spark, tpchSchemaProvider), outputDir, escapeClassName(query.getClass.getName))
    val t1 = System.nanoTime()

    (t1 - t0) / 1000000
  }

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
  }

  def main(args: Array[String]): Unit = {
    // read files from local FS
    var inputDir = "file://" + new File(".").getAbsolutePath() + "/dbgen"
    // read from hdfs
    // val inputDir = "/dbgen"

    // if set write results to hdfs, if null write to stdout
    var outputDir: String = null
    // val outputDir = "/tpch"

    val spark = SparkSession.builder().appName("TPC-H").getOrCreate()

    if (args.length < 1)
      throw new RuntimeException("Invalid number of arguments")

    var queryNum = args(0)
    var fromNum = -1
    var toNum = -1

    if (queryNum.contains("-")) {
      val items = queryNum.split("-")
      fromNum = items(0).toInt
      toNum = items(1).toInt
    } else {
      toNum = queryNum.toInt
    }

    if (args.length > 1)
      inputDir = args(1)
    if (args.length > 2)
      outputDir = args(2)

    var cache = false
    if (args.length > 3)
      if (args(3) == "cache")
        cache = true

    var sql = false
    if (args.length > 4)
      if (args(4) == "sql")
        sql = true

    val schemaProvider = new TpchSchemaProvider(spark, inputDir, cache, sql)


    if (fromNum == -1) {
      println(f"Q${toNum}%02d:" + executeQuery(toNum, spark, schemaProvider, outputDir))
    } else {
      val elapsedTimes = new ListBuffer[Long]()

      for (num <- fromNum to toNum) {
        elapsedTimes += executeQuery(num, spark, schemaProvider, outputDir)
      }
      for (num <- fromNum to toNum) {
        println(f"Q${num}%02d:" + elapsedTimes(num-fromNum))
      }
    }

    schemaProvider.close()
  }
}
