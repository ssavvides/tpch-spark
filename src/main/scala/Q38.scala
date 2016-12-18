package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q38 extends TpchQuery
{
  @Override
  override def getName(): String = "top_100_parts_filter"

  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT l_partkey, SUM(l_quantity), SUM(l_extendedprice), MIN(l_discount), MAX(l_discount) " +
      "FROM lineitem WHERE l_shipdate BETWEEN '1996-01-15' AND '1998-03-15' " +
      "GROUP BY l_partkey ORDER BY SUM(l_quantity) DESC LIMIT 100")
  }
}
