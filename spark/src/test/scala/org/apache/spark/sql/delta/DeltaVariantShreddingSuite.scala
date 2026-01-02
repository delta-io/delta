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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils, TestsStatistics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.{GroupType, MessageType, Type}
import org.scalatest.Ignore

// TODO: Re-enable this test suite after fixing Variant type issues in Spark 4.1.1+
@Ignore
class DeltaVariantShreddingSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with TestsStatistics {

  import testImplicits._

  private def numShreddedFiles(path: String, validation: GroupType => Boolean = _ => true): Int = {
    def listParquetFilesRecursively(dir: String): Seq[String] = {
      val deltaLog = DeltaLog.forTable(spark, dir)
      val files = deltaLog.snapshot.allFiles
      files.collect().map { file: AddFile =>
        file.absolutePath(deltaLog).toString
      }
    }

    val parquetFiles = listParquetFilesRecursively(path)

    def hasStructWithFieldNamesInternal(schema: List[Type], fieldNames: Set[String]): Boolean = {
      schema.exists {
        case group: GroupType if group.getFields.asScala.map(_.getName).toSet == fieldNames =>
          true
        case group: GroupType =>
          hasStructWithFieldNamesInternal(group.getFields.asScala.toList, fieldNames)
        case _ => false
      }
    }

    def hasStructWithFieldNames(schema: MessageType, fieldNames: Set[String]): Boolean = {
      schema.getFields.asScala.exists {
        case group: GroupType if group.getFields.asScala.map(_.getName).toSet == fieldNames &&
          validation(group) =>
          true
        case group: GroupType =>
          hasStructWithFieldNamesInternal(group.getFields.asScala.toList, fieldNames)
        case _ => false
      }
    }

    val requiredFieldNames = Set("value", "metadata", "typed_value")
    val conf = new Configuration()
    parquetFiles.count { p =>
      val reader = ParquetFileReader.open(conf, new Path(p))
      val footer: ParquetMetadata = reader.getFooter
      val isShredded =
        hasStructWithFieldNames(footer.getFileMetaData().getSchema, requiredFieldNames)
      reader.close()
      isShredded
    }
  }

  test("variant shredding table property") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("tbl"))
      assert(!snapshot.protocol
        .isFeatureSupported(VariantShreddingPreviewTableFeature),
        s"Table tbl contains ShreddedVariantTableFeature descriptor when its not supposed to"
      )
      sql(s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'true')")
      assert(getProtocolForTable("tbl")
        .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
    }
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA " +
        s"TBLPROPERTIES('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'true')")
      assert(getProtocolForTable("tbl")
        .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
    }
    assert(DeltaConfigs.ENABLE_VARIANT_SHREDDING.key == "delta.enableVariantShredding")
  }

  test("Spark can read shredded table containing the shredding table feature") {
    withTable("tbl") {
      withTempDir { dir =>
        val schema = "a int, b string, c decimal(15, 1)"
        val df = spark.sql(
          """
            | select id i, case
            | when id = 0 then parse_json('{"a": 1, "b": "2", "c": 3.3, "d": 4.4}')
            | when id = 1 then parse_json('{"a": [1,2,3], "b": "hello", "c": {"x": 0}}')
            | when id = 2 then parse_json('{"A": 1, "c": 1.23}')
            | end v from range(0, 3, 1, 1)
            |""".stripMargin)

        sql("CREATE TABLE tbl (i long, v variant) USING DELTA " +
          s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'true') " +
          s"LOCATION '${dir.getAbsolutePath}'")
        assert(getProtocolForTable("tbl")
          .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
        withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
          SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
          SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {

          df.write.format("delta").mode("append").saveAsTable("tbl")
          // Make sure the actual parquet files are shredded
          assert(numShreddedFiles(dir.getAbsolutePath, validation = { field: GroupType =>
            field.getName == "v" && (field.getType("typed_value") match {
              case t: GroupType =>
                t.getFields.asScala.map(_.getName).toSet == Set("a", "b", "c")
              case _ => false
            })
          }) == 1)
          checkAnswer(
            spark.read.format("delta").load(dir.getAbsolutePath).selectExpr("i", "to_json(v)"),
            df.selectExpr("i", "to_json(v)").collect()
          )
        }
      }
    }
  }

  test("Test shredding property controls shredded writes") {
    val schema = "a int, b string, c decimal(15, 1)"
    val df = spark.sql(
      """
        | select id i, case
        | when id = 0 then parse_json('{"a": 1, "b": "2", "c": 3.3, "d": 4.4}')
        | when id = 1 then parse_json('{"a": [1,2,3], "b": "hello", "c": {"x": 0}}')
        | when id = 2 then parse_json('{"A": 1, "c": 1.23}')
        | end v from range(0, 3, 1, 1)
        |""".stripMargin)
    // Table property not present or false
    Seq("", s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'false') ")
      .foreach { tblProperties =>
        withTable("tbl") {
          withTempDir { dir =>
            sql("CREATE TABLE tbl (i long, v variant) USING DELTA " + tblProperties +
              s"LOCATION '${dir.getAbsolutePath}'")
            withSQLConf(SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> true.toString,
              SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> true.toString,
              SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> schema) {

              val e = intercept[DeltaSparkException] {
                df.write.format("delta").mode("append").saveAsTable("tbl")
              }
              checkError(e, "DELTA_SHREDDING_TABLE_PROPERTY_DISABLED", parameters = Map())
              assert(e.getMessage.contains(
                "Attempted to write shredded Variants but the table does not support shredded " +
                  "writes. Consider setting the table property enableVariantShredding to true."))
              assert(numShreddedFiles(dir.getAbsolutePath, validation = { field: GroupType =>
                field.getName == "v" && (field.getType("typed_value") match {
                  case t: GroupType =>
                    t.getFields.asScala.map(_.getName).toSet == Set("a", "b", "c")
                  case _ => false
                })
              }) == 0)
              checkAnswer(
                spark.read.format("delta").load(dir.getAbsolutePath).selectExpr("i", "to_json(v)"),
                Seq()
              )
            }
          }
        }
      }
  }

  test("Set table property to invalid value") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("tbl"))
      assert(!snapshot.protocol
        .isFeatureSupported(VariantShreddingPreviewTableFeature),
        s"Table tbl contains ShreddedVariantTableFeature descriptor when its not supposed to"
      )
      checkError(
        intercept[SparkException] {
          sql(s"ALTER TABLE tbl " +
            s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_VARIANT_SHREDDING.key}' = 'bla')")
        },
        "_LEGACY_ERROR_TEMP_2045",
        parameters = Map(
          "message" -> "For input string: \"bla\""
        )
      )
      assert(!getProtocolForTable("tbl")
        .readerAndWriterFeatures.contains(VariantShreddingPreviewTableFeature))
    }
  }

  test("creating table with preview feature does not add stable feature (and vice versa)") {
    Seq("-preview", "").foreach { featureSuffix =>
      withTable("tbl") {
        sql(s"""CREATE TABLE tbl(i INT)
                USING delta
                TBLPROPERTIES(
                  'delta.enableVariantShredding' = 'true',
                  'delta.feature.variantShredding$featureSuffix' = 'supported'
                )""")
        DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
          spark,
          "tbl",
          expectPreviewFeature = featureSuffix.nonEmpty,
          expectStableFeature = featureSuffix.isEmpty)
      }
    }
  }

  test("manually enabling preview and stable table feature") {
    Seq(false, true).foreach { forcePreview =>
      withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_SHREDDING_FEATURE.key -> forcePreview.toString) {
        withTable("tbl") {
          sql("""CREATE TABLE tbl(i INT)
              USING delta
              TBLPROPERTIES(
                'delta.feature.variantShredding' = 'supported',
                'delta.feature.variantShredding-preview' = 'supported'
              )""")
          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = true,
            expectStableFeature = true)
          sql("""ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableVariantShredding' = 'true')""")
          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = true,
            expectStableFeature = true)
        }
      }
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_SHREDDING_FEATURE' adds preview table feature for new table") {
    Seq(false, true).foreach { forcePreview =>
      withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_SHREDDING_FEATURE.key -> forcePreview.toString) {
        withTable("tbl") {
          sql("CREATE TABLE tbl(s STRING) USING DELTA TBLPROPERTIES " +
            "('delta.enableVariantShredding' = 'true')")
          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = forcePreview,
            expectStableFeature = !forcePreview)
        }
      }
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_SHREDDING_FEATURE' and setting shredding table property " +
    "adds the preview table feature") {
    Seq(false, true).foreach { forcePreview =>
      withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_SHREDDING_FEATURE.key -> forcePreview.toString) {
        withTable("tbl") {
          sql("CREATE TABLE tbl(s STRING) USING DELTA")
          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = false,
            expectStableFeature = false)

          sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableVariantShredding' = 'true')")

          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = forcePreview,
            expectStableFeature = !forcePreview)
        }
      }
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_VARIANT_FEATURE' on table with stable feature does not " +
    "require adding preview feature") {
    Seq(false, true).foreach { forcePreview =>
      withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_SHREDDING_FEATURE.key -> forcePreview.toString) {
        withTable("tbl") {
          sql("CREATE TABLE tbl(s STRING) USING DELTA TBLPROPERTIES " +
            "('delta.enableVariantShredding' = 'true')")
          DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
            spark,
            "tbl",
            expectPreviewFeature = forcePreview,
            expectStableFeature = !forcePreview)

          withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_VARIANT_FEATURE.key ->
            (!forcePreview).toString) {
            // Reset the table property and set it again to see if it modifies to protocol
            sql("ALTER TABLE tbl SET " +
              "TBLPROPERTIES ('delta.enableVariantShredding' = 'false')")
            DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
              spark,
              "tbl",
              expectPreviewFeature = forcePreview,
              expectStableFeature = !forcePreview)
            sql("ALTER TABLE tbl SET " +
              "TBLPROPERTIES ('delta.enableVariantShredding' = 'true')")
            DeltaVariantShreddingSuite.assertVariantShreddingTableFeatures(
              spark,
              "tbl",
              expectPreviewFeature = forcePreview,
              expectStableFeature = !forcePreview)
          }
        }
      }
    }
  }
}

object DeltaVariantShreddingSuite {
  def assertVariantShreddingTableFeatures(
      spark: SparkSession,
      tableName: String,
      expectPreviewFeature: Boolean,
      expectStableFeature: Boolean): Unit = {
    val features = DeltaLog.forTable(spark, TableIdentifier(tableName)).update().protocol
      .readerAndWriterFeatures
    assert(expectPreviewFeature == features.contains(VariantShreddingPreviewTableFeature))
    assert(expectStableFeature == features.contains(VariantShreddingTableFeature))
  }
}
