package main.scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q31 extends TpchQuery
{
  @Override
  override def getName(): String = "count_star_interval"

  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql("SELECT COUNT(*) FROM lineitem WHERE l_shipdate BETWEEN '1992-01-03' AND '1998-11-30'")
  }
}
