package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 17
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q17 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import schemaProvider._
    import spark.implicits._

    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
  }

}
