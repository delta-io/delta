/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.net.URI

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaTableUtilsSuite extends SharedSparkSession with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.hadoop.fs.s3.impl", classOf[MockS3FileSystem].getCanonicalName)

  test("findDeltaTableRoot correctly combines paths") {
    val path1 = new Path("s3://my-bucket")
    assert(DeltaTableUtils.findDeltaTableRoot(spark, path1).isEmpty)
    val path2 = new Path("s3://my-bucket/")
    assert(DeltaTableUtils.findDeltaTableRoot(spark, path2).isEmpty)
    withTempDir { dir =>
      sql(s"CREATE TABLE myTable (id INT) USING DELTA LOCATION '${dir.getAbsolutePath}'")
      val path = new Path(s"file://${dir.getAbsolutePath}")
      assert(DeltaTableUtils.findDeltaTableRoot(spark, path).contains(path))
    }
  }

  test("safeConcatPaths") {
    val basePath = new Path("s3://my-bucket/subfolder")
    val basePathEmpty = new Path("s3://my-bucket")
    assert(DeltaTableUtils.safeConcatPaths(basePath, "_delta_log") ==
      new Path("s3://my-bucket/subfolder/_delta_log"))
    assert(DeltaTableUtils.safeConcatPaths(basePathEmpty, "_delta_log") ==
      new Path("s3://my-bucket/_delta_log"))
  }

  test("removeInternalMetadata") {
    for (flag <- BOOLEAN_DOMAIN) {
      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_REMOVE_SPARK_INTERNAL_METADATA.key -> flag.toString) {
        for (internalMetadataKey <- DeltaTableUtils.SPARK_INTERNAL_METADATA_KEYS) {
          val metadata = new MetadataBuilder()
            .putString(internalMetadataKey, "foo")
            .putString("other", "bar")
            .build()
          val schema = StructType(Seq(StructField("foo", StringType, metadata = metadata)))
          val newSchema = DeltaTableUtils.removeInternalMetadata(spark, schema)
          newSchema.foreach { f =>
            if (flag) {
              // Flag on: should remove internal metadata
              assert(!f.metadata.contains(internalMetadataKey))
              // Should reserve non internal metadata
              assert(f.metadata.contains("other"))
            } else {
              // Flag off: no-op
              assert(f.metadata == metadata)
            }
          }
        }
      }
    }
  }
}

private class MockS3FileSystem extends RawLocalFileSystem {
  override def getScheme: String = "s3"
  override def getUri: URI = URI.create("s3://my-bucket")
}
