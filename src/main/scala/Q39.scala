package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q39 extends TpchQuery
{
  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT l_commitdate, SUM(l_quantity) FROM lineitem " +
      "GROUP BY l_commitdate ORDER BY SUM(l_quantity) DESC LIMIT 100")
  }
}
