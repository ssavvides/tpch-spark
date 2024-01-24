package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

class TpchSchemaProvider(spark: SparkSession, inputDir: String) {
  // TPC-H table schemas
  private val dfSchemaMap = Map(
    "customer" -> StructType(
      StructField("c_custkey", LongType) :: // primary key
        StructField("c_name", StringType) ::
        StructField("c_address", StringType) ::
        StructField("c_nationkey", LongType) ::
        StructField("c_phone", StringType) ::
        StructField("c_acctbal", DoubleType) ::
        StructField("c_mktsegment", StringType) ::
        StructField("c_comment", StringType) :: Nil),
    "lineitem" -> StructType(
      StructField("l_orderkey", LongType) :: // primary key
        StructField("l_partkey", LongType) ::
        StructField("l_suppkey", LongType) ::
        StructField("l_linenumber", LongType) :: // primary key
        StructField("l_quantity", DoubleType) ::
        StructField("l_extendedprice", DoubleType) ::
        StructField("l_discount", DoubleType) ::
        StructField("l_tax", DoubleType) ::
        StructField("l_returnflag", StringType) ::
        StructField("l_linestatus", StringType) ::
        StructField("l_shipdate", StringType) ::
        StructField("l_commitdate", StringType) ::
        StructField("l_receiptdate", StringType) ::
        StructField("l_shipinstruct", StringType) ::
        StructField("l_shipmode", StringType) ::
        StructField("l_comment", StringType) :: Nil),
    "nation" -> StructType(
      StructField("n_nationkey", LongType) :: // primary key
        StructField("n_name", StringType) ::
        StructField("n_regionkey", LongType) ::
        StructField("n_comment", StringType) :: Nil),
    "orders" -> StructType(
      StructField("o_orderkey", LongType) :: // primary key
        StructField("o_custkey", LongType) ::
        StructField("o_orderstatus", StringType) ::
        StructField("o_totalprice", DoubleType) ::
        StructField("o_orderdate", StringType) ::
        StructField("o_orderpriority", StringType) ::
        StructField("o_clerk", StringType) ::
        StructField("o_shippriority", LongType) ::
        StructField("o_comment", StringType) :: Nil),
    "part" -> StructType(
      StructField("p_partkey", LongType) :: // primary key
        StructField("p_name", StringType) ::
        StructField("p_mfgr", StringType) ::
        StructField("p_brand", StringType) ::
        StructField("p_type", StringType) ::
        StructField("p_size", LongType) ::
        StructField("p_container", StringType) ::
        StructField("p_retailprice", DoubleType) ::
        StructField("p_comment", StringType) :: Nil),
    "partsupp" -> StructType(
      StructField("ps_partkey", LongType) :: // primary key
        StructField("ps_suppkey", LongType) :: // primary key
        StructField("ps_availqty", LongType) ::
        StructField("ps_supplycost", DoubleType) ::
        StructField("ps_comment", StringType) :: Nil),
    "region" -> StructType(
      StructField("r_regionkey", LongType) :: // primary key
        StructField("r_name", StringType) ::
        StructField("r_comment", StringType) :: Nil),
    "supplier" -> StructType(
      StructField("s_suppkey", LongType) :: // primary key
        StructField("s_name", StringType) ::
        StructField("s_address", StringType) ::
        StructField("s_nationkey", LongType) ::
        StructField("s_phone", StringType) ::
        StructField("s_acctbal", DoubleType) ::
        StructField("s_comment", StringType) :: Nil)
  )

  private val dfMap = dfSchemaMap.map {
    case (name, schema) => (name, spark.read.schema(schema).option("delimiter", "|").csv(inputDir + s"/$name.tbl*"))
  }

  // for implicits
  val customer: DataFrame = dfMap("customer")
  val lineitem: DataFrame = dfMap("lineitem")
  val nation: DataFrame = dfMap("nation")
  val region: DataFrame = dfMap("region")
  val order: DataFrame = dfMap("orders")
  val part: DataFrame = dfMap("part")
  val partsupp: DataFrame = dfMap("partsupp")
  val supplier: DataFrame = dfMap("supplier")

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
