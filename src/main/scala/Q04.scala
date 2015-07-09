package main.scala

import org.apache.spark.sql.functions.count

/**
 * TPC-H Query 4
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q04 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val forders = order.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey").distinct

    val res = flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")

    res.collect().foreach(println)

  }

}
