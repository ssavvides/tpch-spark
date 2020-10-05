package org.tpch.tablereader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row}
import scala.reflect.runtime.universe._

object TpchTableReader {
  
  private val s3IpAddr = "minioserver"
  private val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("TpchProvider")
      .config("spark.datasource.s3.endpoint", s"""http://$s3IpAddr:9000""")
      .config("spark.datasource.s3.accessKey", "admin")
      .config("spark.datasource.s3.secretKey", "admin123")
      .getOrCreate()

  private val dfRead = sparkSession.read
      .format("org.apache.spark.sql.execution.datasources.v2.s3")
      .option("format", "csv")

  def readTable[T: WeakTypeTag]
               (name: String, inputDir: String, s3Select: Boolean)
               (implicit tag: TypeTag[T]): Dataset[Row] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    if (!s3Select) {
      val df = sparkSession.read
        .format("org.apache.spark.sql.execution.datasources.v2.s3")
        .option("format", "csv")
        .option("DisablePushDown", "")
        .schema(schema)
        .load(inputDir + "/" +  name + ".csv*")
        // df.show()
        df
    } else {
      val df = sparkSession.read
        .format("org.apache.spark.sql.execution.datasources.v2.s3")
        .option("format", "csv")
        .schema(schema)
        .load(inputDir + "/" +  name + ".csv*")
        // df.show()
        df
    }
  }
}
