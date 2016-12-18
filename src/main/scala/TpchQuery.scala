package main.scala

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
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
  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)
  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}


object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
  }

  def executeQueries(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider, fromNum: Int, toNum: Int, runs: Int) : ListBuffer[(String, Float)] = {
    val results = new ListBuffer[(String, Float)]

    for (queryNo <- fromNum to toNum) {
      for (i <- 1 to runs) {
        val t0 = System.nanoTime()

        val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
        outputDF(query.execute(spark, tpchSchemaProvider), tpchSchemaProvider.outputDir, query.getName())

        val t1 = System.nanoTime()

        val elapsed = (t1 - t0) / 1000000000.0f // second
        results += new Tuple2(query.getName(), elapsed)
      }
    }

    return results
  }

  case class Config(appName: String = "TPC-H", runs: Int = 1, input: String = "", output: String = "",
    resultfile: String = "", fromNum: Int = -1, toNum: Int = -1, queryNum: Int = -1, caches: Seq[String] = Seq(),
    sql: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("TpchBenchmark") {
      head("TPC-H Benchmark for Spark")

      opt[String]("appName").action( (x,c) => c.copy(appName = x)).
        text("spark application name")
      opt[Int]('r', "runs").action( (x,c) => c.copy(runs = x)).
        text("how many times to run queries")
      opt[String]('i', "input").required().action( (x, c) => c.copy(input = x)).
        text("input is the path for the source directory. file:// or hdfs://")
      opt[String]('o', "output").action( (x, c) => c.copy(output = x)).
        text("output is the path for the target directory. file:// or hdfs://")
      opt[String]("result").required().action( (x,c) => c.copy(resultfile = x)).
        text("result tsv file")
      opt[Int]('f', "from").action( (x,c) => c.copy(fromNum = x)).
        text("from is the number from which the query run starts")
      opt[Int]('t', "to").action( (x,c) => c.copy(toNum = x)).
        text("to is the number the query run ends")
      opt[Int]('q', "query").action( (x,c) => c.copy(queryNum = x)).
        text("query is the individual number to run")
      opt[Seq[String]]("caches").valueName("customer,customer...").action( (x,c) =>
        c.copy(caches = x)) .text("tables to cache to run queris upon. 'all' makes the whole table cached")
      opt[Boolean]("sql").action( (x,c) => c.copy(sql = x)).
        text("the sql flag indicates whether the queries are run by means of sql or dataframe")

      help("help").text("prints this usage text")

      checkConfig(c =>
        if (c.queryNum != -1 && c.fromNum != -1 && c.toNum != -1) {
          failure("query and from/to are not set at the same time")
        } else {
          success
        }
      )
    }

    val config = parser.parse(args, Config()) match {
      case Some(config) =>
        config
      case None =>
        print("failed to parse arguments")
        return
    }

    val spark = SparkSession.builder().appName(config.appName).getOrCreate()
    val schemaProvider = new TpchSchemaProvider(spark, config.input, config.output, config.caches, config.sql)

    val output = new ListBuffer[(String, Float)]
    val results = if (config.queryNum != -1) {
      output ++= executeQueries(spark, schemaProvider, config.queryNum, config.queryNum, config.runs)
    } else {
     output ++= executeQueries(spark, schemaProvider, config.fromNum, config.toNum, config.runs)
    }

    schemaProvider.close()

    val outFile = new File(config.resultfile)
    val bw = new BufferedWriter(new FileWriter(outFile, false))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
