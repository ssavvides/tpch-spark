package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 13
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q13 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import schemaProvider._

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
  }

}
