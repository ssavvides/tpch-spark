package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.min

/**
 * TPC-H Query 2
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q02 extends TpchQuery {
  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import schemaProvider._
    import spark.implicits._

    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
  }

}
