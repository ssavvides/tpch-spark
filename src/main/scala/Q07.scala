package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 7
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q07 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // cache fnation

    val fnation = nation.filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")
    val fline = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

    val supNation = fnation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(fline, $"s_suppkey" === fline("l_suppkey"))
      .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

    fnation.join(customer, $"n_nationkey" === customer("c_nationkey"))
      .join(order, $"c_custkey" === order("o_custkey"))
      .select($"n_name".as("cust_nation"), $"o_orderkey")
      .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
      .filter($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY"
        || $"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE")
      .select($"supp_nation", $"cust_nation",
        getYear($"l_shipdate").as("l_year"),
        decrease($"l_extendedprice", $"l_discount").as("volume"))
      .groupBy($"supp_nation", $"cust_nation", $"l_year")
      .agg(sum($"volume").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")
  }

}
