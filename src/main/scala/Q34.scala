package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q34 extends TpchQuery
{
  @Override
  override def getName(): String = "sum_all_year"

  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT YEAR(l_shipdate), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax), SUM(l_quantity) " +
      "FROM lineitem GROUP BY YEAR(l_shipdate)")
  }
}
