package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 11
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q11 extends TpchQuery {
  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import schemaProvider._
    import spark.implicits._

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)
  }

}
