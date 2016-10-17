package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Q40 extends TpchQuery
{
  @Override
  override def getName(): String = "tpch-query-1"

  override def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame = {
    spark.sql(
      "SELECT " +
        "l_returnflag," +
        "l_linestatus," +
        "sum(l_quantity) as sum_qty," +
        "sum(l_extendedprice) as sum_base_price," +
        "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price," +
        "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge," +
        "avg(l_quantity) as avg_qty," +
        "avg(l_extendedprice) as avg_price," +
        "avg(l_discount) as avg_disc," +
        "count(*) as count_order " +
      "FROM lineitem " +
      "WHERE l_shipdate <= date_sub('1998-12-01', 90) " +
      "GROUP BY l_returnflag, l_linestatus " +
      "ORDER BY l_returnflag, l_linestatus")
  }
}
