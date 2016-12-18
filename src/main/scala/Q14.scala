package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 14
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q14 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import schemaProvider._
    import spark.implicits._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))
  }

}
