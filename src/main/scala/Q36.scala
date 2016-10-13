package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q36 extends TpchQuery
{
  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT l_partkey, SUM(l_quantity) FROM lineitem " +
      "GROUP BY l_partkey ORDER BY SUM(l_quantity) DESC LIMIT 100")
  }
}
