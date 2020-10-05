package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import org.apache.spark.sql.catalyst.ScalaReflection
import scopt.OParser
import org.apache.hadoop.fs._
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
    val items = className.split("\\.")
    val last = items(items.length-1)
    println(last)
    last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(sc: SparkContext, 
                     schemaProvider: TpchSchemaProvider, 
                     queryNum: Int): ListBuffer[(String, Float)] = {

    val OUTPUT_DIR: String = "file:///build/tpch-test-output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d")
                       .newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  case class Config(
    start: Int = 0,
    var end: Int = -1,
    var fileType: FileType = CSV,
    test: String = "csv",
    var init: Boolean = false,
    s3Select: Boolean = false,
    kwargs: Map[String, String] = Map())
  
  val maxTests = 22
  def parseArgs(args: Array[String]): Config = {
  
    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("TCPH Benchmark"),
        head("tpch-test", "0.1"),
        // option -f, --foo
        opt[Int]('s', "start")
          .action((x, c) => c.copy(start = x.toInt))
          .text("start test number"),
        opt[String]("test")
          .required
          .action((x, c) => c.copy(test = x))
          .text("test to run (csv, tbl, init"),
        opt[Unit]("s3Select")
          .action((x, c) => c.copy(s3Select = true))
          .text("Enable s3Select pushdown, default is disabled."),
      )
    }
    // OParser.parse returns Option[Config]
    val config = OParser.parse(parser1, args, Config())
            
    config match {
        case Some(config) => 
          println("args are good")

          config.test match {
            case "csv" => config.fileType = CSV
            case "tbl" => config.fileType = TBL
            case "init" => {
              config.init = true
              config.fileType = TBL
            }
          }
          config.end = config.start
          config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new Config
      }
  }
  val inputTblPath = "file:///build/tpch-data"
  def benchmark(config: Config): Unit = {
        
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // read files from local FS
    val inputPath = if (config.test == "csv") "s3a://tpch-test" else inputTblPath

    val schemaProvider = new TpchSchemaProvider(sc, inputPath, 
                                                config.s3Select, config.fileType)
    for (i <- config.start to config.end) {
      val output = new ListBuffer[(String, Float)]
      output ++= executeQueries(sc, schemaProvider, i)

      val outFile = new File("TIMES" + i + ".txt")
      val bw = new BufferedWriter(new FileWriter(outFile, true))

      output.foreach {
        case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
      }

      bw.close()
    }
  }

  def init(config: Config): Unit = {
        
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val schemaProvider = new TpchSchemaProvider(sc, inputTblPath, 
                                                config.s3Select, config.fileType)

    for ((name, df) <- schemaProvider.dfMap) {
      val outputFolder = "/build/tpch-data/" + name + "raw"
      df.repartition(1)
        .write
        .option("header", true)
        .format("csv")
        .save(outputFolder)

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val file = fs.globStatus(new Path(outputFolder + "/part-0000*"))(0).getPath().getName()
      println("\n\n\n\nfound " + file)
      println(outputFolder + "/" + file + "->" + "/build/tpch-data/" + name + ".csv")
      fs.rename(new Path(outputFolder + "/" + file), new Path("/build/tpch-data/" + name + ".csv"))
      println("Finished writing " + name + ".csv")
    }
    println("Finished converting *.tbl to *.csv")
  }

  def main(args: Array[String]): Unit = {

    var s3Select = false;
    val config = parseArgs(args) 
    println("s3Select: " + config.s3Select)
    println("test: " + config.test)
    println("fileType: " + config.fileType)
    println("start: " + config.start)
    println("end: " + config.end)
    
    if (config.test == "init") {
      init(config)
    } else {
      benchmark(config)
    }
  }
}
