/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConfV2}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.UCTableInjectingSessionCatalog

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.exceptions.TestFailedException

/**
 * Test suite for V2 streaming conversion logic via ApplyV2Streaming rule.
 * Tests the interaction between V2_ENABLE_MODE configuration and streaming queries.
 */
class V2StreamingConversionSuite
  extends StreamTest
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  /**
   * Helper to check if a DataFrame's logical plan uses StreamingRelationV2 (V2 path).
   */
  private def usesV2Streaming(df: org.apache.spark.sql.DataFrame): Boolean = {
    df.queryExecution.analyzed.collectFirst {
      case _: StreamingRelationV2 => true
    }.isDefined
  }

  /**
   * Helper to check if a DataFrame's logical plan uses StreamingRelation (V1 path).
   */
  private def usesV1Streaming(df: org.apache.spark.sql.DataFrame): Boolean = {
    df.queryExecution.analyzed.collectFirst {
      case _: StreamingRelation => true
    }.isDefined
  }

  // ============================================================================
  // AUTO Mode Tests
  // ============================================================================

  test("AUTO mode: UC-managed table via test catalog runs V2 streaming (expected to fail today)") {
    // TODO: Enable this as a passing end-to-end test once the kernel-backed streaming source
    // supports initialOffset with an initial snapshot (SparkMicroBatchStream limitation).
    withTable("uc_table") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        // Create a real table in the metastore first, under the default catalog setup.
        // We use LOCATION to avoid relying on managed-location behaviors of the test catalog.
        sql(s"CREATE TABLE uc_table (id LONG) USING delta LOCATION '$path'")
        sql("INSERT INTO uc_table VALUES (1)")

        // Reset catalog manager so the new `spark_catalog` implementation can apply.
        spark.sessionState.catalogManager.reset()
        try {
          withSQLConf(
            "spark.sql.catalog.spark_catalog" -> classOf[UCTableInjectingSessionCatalog].getName,
            DeltaSQLConfV2.V2_ENABLE_MODE.key -> "AUTO") {
            // Reset again to force catalog re-initialization with updated configs.
            spark.sessionState.catalogManager.reset()

            val df = spark.readStream.table("uc_table")
            assert(usesV2Streaming(df),
              "AUTO mode should use V2 streaming for UC-managed tables (via test catalog)")

            val e = intercept[TestFailedException] {
              testStream(df)(ProcessAllAvailable())
            }
            assert(
              e.getMessage.contains("initialOffset with initial snapshot is not supported yet"),
              s"Unexpected failure; message was: ${e.getMessage}"
            )
          }
        } finally {
          spark.sessionState.catalogManager.reset()
        }
      }
    }
  }

  test("AUTO mode: non-UC catalog table uses V1 streaming (DeltaLog)") {
    withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> "AUTO") {
      withTable("regular_table") {
        // Create regular catalog table (no UC properties)
        sql("CREATE TABLE regular_table (id INT, value STRING) USING delta")
        sql("INSERT INTO regular_table VALUES (0, 'init')")

        // Create streaming read
        val df = spark.readStream.table("regular_table")

        // Verify V1 streaming is used (no UC table ID)
        assert(usesV1Streaming(df),
          "AUTO mode should use V1 streaming (StreamingRelation) for non-UC catalog tables")

        // Verify streaming works correctly
        testStream(df)(
          Execute { _ =>
            sql("INSERT INTO regular_table VALUES (1, 'a')")
          },
          ProcessAllAvailable(),
          CheckAnswer((0, "init"), (1, "a"))
        )
      }
    }
  }

  test("AUTO mode: path-based table uses V1 streaming") {
    withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> "AUTO") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath

        // Create path-based table
        spark.range(3).selectExpr("id", "CAST(id AS STRING) as value")
          .write.format("delta").save(path)

        // Create streaming read from path
        val df = spark.readStream
          .format("delta")
          .load(path)

        // Verify V1 streaming is used (no catalog table)
        assert(usesV1Streaming(df),
          "AUTO mode should use V1 streaming for path-based tables")

        // Verify streaming works
        testStream(df)(
          Execute { _ =>
            spark.range(3, 5).selectExpr("id", "CAST(id AS STRING) as value")
              .write.format("delta").mode("append").save(path)
          },
          ProcessAllAvailable(),
          CheckAnswer((0, "0"), (1, "1"), (2, "2"), (3, "3"), (4, "4"))
        )
      }
    }
  }

  // ============================================================================
  // NONE Mode Tests (Default behavior)
  // ============================================================================

  test("NONE mode: all tables use V1 streaming") {
    withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> "NONE") {
      withTable("test_table") {
        sql("CREATE TABLE test_table (id INT) USING delta")
        sql("INSERT INTO test_table VALUES (1)")

        val df = spark.readStream.table("test_table")

        assert(usesV1Streaming(df),
          "NONE mode should use V1 streaming for all tables")

        testStream(df)(
          Execute { _ =>
            sql("INSERT INTO test_table VALUES (2)")
          },
          ProcessAllAvailable(),
          CheckAnswer(1, 2)
        )
      }
    }
  }

  test("NONE mode: UC-managed table via test catalog is not rewritten to V2 streaming") {
    withTable("uc_table") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        sql(s"CREATE TABLE uc_table (id INT) USING delta LOCATION '$path'")
        sql("INSERT INTO uc_table VALUES (1)")

        spark.sessionState.catalogManager.reset()
        try {
          withSQLConf(
            "spark.sql.catalog.spark_catalog" -> classOf[UCTableInjectingSessionCatalog].getName,
            DeltaSQLConfV2.V2_ENABLE_MODE.key -> "NONE") {
            spark.sessionState.catalogManager.reset()

            val df = spark.readStream.table("uc_table")
            assert(usesV1Streaming(df),
              "NONE mode should not rewrite to V2 streaming even for UC-managed tables")

            // Still validate the source executes as V1 streaming.
            testStream(df)(
              Execute { _ =>
                sql("INSERT INTO uc_table VALUES (2)")
              },
              ProcessAllAvailable(),
              CheckAnswer(1, 2)
            )
          }
        } finally {
          spark.sessionState.catalogManager.reset()
        }
      }
    }
  }

  // ============================================================================
  // STRICT Mode Tests
  // ============================================================================

  test("STRICT mode: catalog table uses V2 streaming via DeltaCatalog") {
    // In STRICT mode, DeltaCatalog returns SparkTable directly (not via ApplyV2Streaming)
    // This causes catalog tables to use V2 streaming
    withTable("strict_test_table") {
      withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> "STRICT") {
        withTempDir { dir =>
          val path = dir.getCanonicalPath

          // Create table using path-based approach to avoid V2 write issues
          spark.range(1).toDF("id").write.format("delta").save(path)

          // Create a catalog table pointing to this location
          sql(s"CREATE TABLE strict_test_table USING delta LOCATION '$path'")

          val df = spark.readStream.table("strict_test_table")

          // In STRICT mode, catalog returns SparkTable, so we use V2 streaming
          assert(usesV2Streaming(df),
            "STRICT mode uses V2 streaming for catalog tables " +
              "(via DeltaCatalog, not ApplyV2Streaming)")

          // TODO: Update this test to verify streaming execution works once SparkMicroBatchStream
          // supports initialOffset with initial snapshot. Currently it fails as expected.
          val e = intercept[TestFailedException] {
            testStream(df)(ProcessAllAvailable())
          }
          assert(
            e.getMessage.contains("initialOffset with initial snapshot is not supported yet"),
            s"STRICT mode V2 streaming should fail with expected error; " +
              s"message was: ${e.getMessage}"
          )
        }
      }
    }
  }

  test("STRICT mode: path-based table uses V1 streaming") {
    withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> "STRICT") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath

        // Create path-based table
        spark.range(3).selectExpr("id", "CAST(id AS STRING) as value")
          .write.format("delta").save(path)

        // Create streaming read from path
        val df = spark.readStream
          .format("delta")
          .load(path)

        // STRICT mode currently uses V1 for path-based tables (no catalog table)
        assert(usesV1Streaming(df),
          "STRICT mode should use V1 streaming for path-based tables (no catalog table)")

        testStream(df)(
          Execute { _ =>
            spark.range(3, 5).selectExpr("id", "CAST(id AS STRING) as value")
              .write.format("delta").mode("append").save(path)
          },
          ProcessAllAvailable(),
          CheckAnswer((0, "0"), (1, "1"), (2, "2"), (3, "3"), (4, "4"))
        )
      }
    }
  }

  // ============================================================================
  // Schema Bypass Logic Tests
  // ============================================================================

  Seq("AUTO", "STRICT").foreach { mode =>
    test(s"sourceSchema: $mode mode falls back to DeltaLog for non-UC tables") {
      withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> mode) {
        withTable("test_table") {
          sql("CREATE TABLE test_table (id INT, value STRING) USING delta")

          val catalogTable = spark.sessionState.catalog.getTableMetadata(
            TableIdentifier("test_table"))

          val dataSource = new DeltaDataSource()
          dataSource.setCatalogTableOpt(Some(catalogTable))

          val providedSchema = StructType(Seq(
            StructField("id", IntegerType),
            StructField("value", StringType)
          ))

          val (shortName, returnedSchema) = dataSource.sourceSchema(
            spark.sqlContext,
            Some(providedSchema),
            "delta",
            Map("path" -> catalogTable.location.toString)
          )

          // For non-UC tables in AUTO/STRICT mode, should load via DeltaLog
          // The returned schema should match the actual table schema
          assert(returnedSchema.fieldNames.contains("id"))
          assert(returnedSchema.fieldNames.contains("value"))
        }
      }
    }

    test(s"sourceSchema: $mode mode uses provided schema for UC managed tables") {
      withSQLConf(DeltaSQLConfV2.V2_ENABLE_MODE.key -> mode) {
        withTable("uc_table") {
          // Create an empty table - we only need valid Delta metadata, not actual data
          sql("CREATE TABLE uc_table (id INT, value STRING) USING delta")

          val catalogTable = spark.sessionState.catalog.getTableMetadata(
            TableIdentifier("uc_table"))

          // Manually inject UC properties to simulate a UC-managed table
          // In production, these would be set by Unity Catalog when the table is created
          val ucCatalogTable = catalogTable.copy(
            storage = catalogTable.storage.copy(
              properties = catalogTable.storage.properties ++ Map(
                "test.simulateUC" -> "true",
                "io.unitycatalog.tableId" -> "test-uc-table-id"
              )
            )
          )

          val dataSource = new DeltaDataSource()
          dataSource.setCatalogTableOpt(Some(ucCatalogTable))

          // Provide a schema with different field order to verify bypass works
          // The bypass should trust the provided schema without validation
          val providedSchema = StructType(Seq(
            StructField("value", StringType),
            StructField("id", IntegerType)
          ))

          val (shortName, returnedSchema) = dataSource.sourceSchema(
            spark.sqlContext,
            Some(providedSchema),
            "delta",
            Map("path" -> catalogTable.location.toString)
          )

          // For UC managed tables in AUTO/STRICT mode, should use provided schema
          // without validation. Verify the returned schema matches the provided schema
          // (different order than table)
          assert(returnedSchema.fieldNames.toSeq == Seq("value", "id"),
            s"$mode mode should use provided schema for UC managed tables, " +
              "preserving field order")
        }
      }
    }
  }
}

