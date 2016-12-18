package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 16
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q16 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import schemaProvider._
    import spark.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part.filter(($"p_brand" !== "Brand#45") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

}
