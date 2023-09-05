package org.apache.spark.sql.delta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class QueryMetadataSuite extends QueryTest
  with SharedSparkSession
  with DeltaDMLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  test("batch _metadata query") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil) // test table

      val metadata = spark.read.format("delta").load(tempPath).select("_metadata").collect()

      // TODO verify the metadata response
    }
  }

  test("streaming _metadata query") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil) // test table

      // TODO breaks
      val metadata = spark.readStream.format("delta").load(tempPath).select("_metadata").collect()

      // TODO verify the metadata response
    }
  }
}
