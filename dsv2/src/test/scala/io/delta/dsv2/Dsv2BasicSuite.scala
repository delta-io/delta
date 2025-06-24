package io.delta.dsv2
import java.util.UUID
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{QueryTest, SparkSession, functions}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class Dsv2BasicSuite extends QueryTest with SharedSparkSession {

  test("test reading using dsv2") {
    val conf = new SparkConf()
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.dsv1", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.sql.catalog.dsv2", "io.delta.dsv2.catalog.DeltaCatalog")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.sql(
      s"CREATE OR REPLACE TABLE delta.`/tmp/spark_warehouse/table`" +
        s" USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4;")
    val exception = intercept[UnsupportedOperationException] {
    sparkSession.sql(
      s"SELECT * FROM dsv2.namespace.table").collect()
    }
    assert(exception.getMessage.contains("todo"))
  }

}
