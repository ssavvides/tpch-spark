package main.scala

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 13
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q13 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    val res = customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)

    res.collect().foreach(println)

  }

}
