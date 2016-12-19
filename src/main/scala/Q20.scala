package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 20
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q20 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val forest = udf { (x: String) => x.startsWith("forest") }

    val flineitem = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")
  }

}
