package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q33 extends TpchQuery
{
  @Override
  override def getName(): String = "sum_all"

  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT SUM(l_extendedprice), SUM(l_discount), SUM(l_tax), SUM(l_quantity) FROM lineitem")
  }
}
