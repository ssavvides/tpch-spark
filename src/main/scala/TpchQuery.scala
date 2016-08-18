package main.scala

import java.io.File
import org.apache.spark.sql._

// TPC-H table schemas
case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // read files from local FS
  val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  // val INPUT_DIR: String = "/dbgen"

  // if set write results to hdfs, if null write to stdout
  // val OUTPUT_DIR: String = "/tpch"
  val OUTPUT_DIR: String = null

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val spark = SparkSession.builder().appName("TPC-H " + className).getOrCreate()
  import spark.implicits._

  val customer = spark.sparkContext.textFile(INPUT_DIR + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
  val lineitem = spark.sparkContext.textFile(INPUT_DIR + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
  val nation = spark.sparkContext.textFile(INPUT_DIR + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
  val region = spark.sparkContext.textFile(INPUT_DIR + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
  val order = spark.sparkContext.textFile(INPUT_DIR + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
  val part = spark.sparkContext.textFile(INPUT_DIR + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
  val partsupp = spark.sparkContext.textFile(INPUT_DIR + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
  val supplier = spark.sparkContext.textFile(INPUT_DIR + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(): Unit

  def outputDF(df: DataFrame): Unit = {

    if (OUTPUT_DIR == null || OUTPUT_DIR == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
  }
}

object TpchQuery {

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[{ def execute }].execute
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 1)
      executeQuery(args(0).toInt)
    else
      throw new RuntimeException("Invalid number of arguments")
  }
}
