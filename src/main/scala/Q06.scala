package main.scala

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 6
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q06 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val res = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))

    outputDF(res)

  }

}
