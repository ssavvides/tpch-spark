package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.min
import scala.reflect.runtime.universe

object Q02 {

  case class Nation(
    n_nationkey: Int,
    n_name: String,
    n_regionkey: Int,
    n_comment: String)

  case class Region(
    r_regionkey: Int,
    r_name: String,
    r_comment: String)

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

  case class Supplier(
    s_suppkey: Int,
    s_name: String,
    s_address: String,
    s_nationkey: Int,
    s_phone: String,
    s_acctbal: Double,
    s_comment: String)

  def main(args: Array[String]) {

    // Set-up context
    val sparkConf = new SparkConf().setAppName("TPC-H Q02")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    // Create an RDD of Person objects and register it as a table.
    val nation = sc.textFile("dbgen/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
    nation.registerTempTable("nation")

    val region = sc.textFile("dbgen/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
    region.registerTempTable("region")

    val part = sc.textFile("dbgen/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
    part.registerTempTable("part")

    val partsupp = sc.textFile("dbgen/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
    partsupp.registerTempTable("partsupp")

    val supplier = sc.textFile("dbgen/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
    supplier.registerTempTable("supplier")

    val europe = region.filter(region("r_name") === "EUROPE")
      .join(nation, region("r_regionkey") === nation("n_regionkey"))
      .join(supplier, nation("n_nationkey") === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")

    val minCost = brass.groupBy(brass("ps_partkey")).agg(min("ps_supplycost"))

    val res = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("MIN(ps_supplycost)"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)

    res.collect().foreach(println)

  }

}
