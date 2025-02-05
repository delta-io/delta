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

import scala.collection.mutable.{Seq => MutableSeq}
import scala.io.Source

import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils, TestsStatistics}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaVariantSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with TestsStatistics {

  import testImplicits._

  private def getProtocolForTable(table: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    deltaLog.unsafeVolatileSnapshot.protocol
  }

  private def assertVariantTypeTableFeatures(
      tableName: String,
      expectPreviewFeature: Boolean,
      expectStableFeature: Boolean): Unit = {
    val features = getProtocolForTable("tbl").readerAndWriterFeatures
    if (expectPreviewFeature) {
      assert(features.contains(VariantTypePreviewTableFeature))
    } else {
      assert(!features.contains(VariantTypePreviewTableFeature))
    }
    if (expectStableFeature) {
      assert(features.contains(VariantTypeTableFeature))
    } else {
      assert(!features.contains(VariantTypeTableFeature))
    }
  }

  test("create a new table with Variant, higher protocol and feature should be picked.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assertVariantTypeTableFeatures(
        "tbl", expectPreviewFeature = false, expectStableFeature = true)
    }
  }

  test("creating a table without Variant should use the usual minimum protocol") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      assert(
        !deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(
          VariantTypePreviewTableFeature) &&
        !deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(
          VariantTypeTableFeature),
        s"Table tbl contains VariantTypeFeature descriptor when its not supposed to"
      )
    }
  }

  test("add a new Variant column should upgrade to the correct protocol versions") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING) USING delta")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      // Should throw error
      val e = intercept[SparkThrowable] {
        sql("ALTER TABLE tbl ADD COLUMN v VARIANT")
      }
      // capture the existing protocol here.
      // we will check the error message later in this test as we need to compare the
      // expected schema and protocol
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val currentProtocol = deltaLog.unsafeVolatileSnapshot.protocol
      val currentFeatures = currentProtocol.implicitlyAndExplicitlySupportedFeatures
        .map(_.name)
        .toSeq
        .sorted
        .mkString(", ")

      // add table feature
      sql(
        s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('delta.feature.variantType' = 'supported')"
      )

      sql("ALTER TABLE tbl ADD COLUMN v VARIANT")

      // check previously thrown error message
      checkError(
        e,
        "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
        parameters = Map(
          "unsupportedFeatures" -> VariantTypeTableFeature.name,
          "supportedFeatures" -> currentFeatures
        )
      )

      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))

      assert(
        getProtocolForTable("tbl") ==
        VariantTypeTableFeature.minProtocolVersion
          .withFeature(VariantTypeTableFeature)
          .withFeature(InvariantsTableFeature)
          .withFeature(AppendOnlyTableFeature)
      )
    }
  }

  test("variant stable and preview features can be supported simultaneously and read") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(v VARIANT) USING delta")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assertVariantTypeTableFeatures(
        "tbl", expectPreviewFeature = false, expectStableFeature = true)
      sql(
        s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('delta.feature.variantType-preview' = 'supported')"
      )
      assertVariantTypeTableFeatures(
        "tbl", expectPreviewFeature = true, expectStableFeature = true)
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
    }
  }

  test("creating a new variant table uses only the stable table feature") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assertVariantTypeTableFeatures(
        "tbl", expectPreviewFeature = false, expectStableFeature = true)
    }
  }

  test("manually adding preview table feature does not require adding stable table feature") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING) USING delta")
      sql(
        s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('delta.feature.variantType-preview' = 'supported')"
      )

      sql("ALTER TABLE tbl ADD COLUMN v VARIANT")

      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))

      assertVariantTypeTableFeatures(
        "tbl",
        expectPreviewFeature = true,
        expectStableFeature = false
      )
    }
  }

  test("creating table with preview feature does not add stable feature") {
    withTable("tbl") {
      sql(s"""CREATE TABLE tbl(v VARIANT)
              USING delta
              TBLPROPERTIES('delta.feature.variantType-preview' = 'supported')"""
      )
      sql("INSERT INTO tbl (SELECT parse_json(cast(id + 99 as string)) FROM range(1))")
      assertVariantTypeTableFeatures(
        "tbl",
        expectPreviewFeature = true,
        expectStableFeature = false
      )
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_VARIANT_FEATURE' adds preview table feature for new table") {
    withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_VARIANT_FEATURE.key -> "true") {
      withTable("tbl") {
        sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
        sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
        assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
        assertVariantTypeTableFeatures(
          "tbl", expectPreviewFeature = true, expectStableFeature = false)
      }
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_VARIANT_FEATURE' and adding a variant column hints to add " +
       " the preview table feature") {
    withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_VARIANT_FEATURE.key -> "true") {
      withTable("tbl") {
        sql("CREATE TABLE tbl(s STRING) USING delta")

        val e = intercept[SparkThrowable] {
          sql("ALTER TABLE tbl ADD COLUMN v VARIANT")
        }

        checkError(
          e,
          "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
          parameters = Map(
            "unsupportedFeatures" -> VariantTypePreviewTableFeature.name,
            "supportedFeatures" -> DeltaLog.forTable(spark, TableIdentifier("tbl"))
              .unsafeVolatileSnapshot
              .protocol
              .implicitlyAndExplicitlySupportedFeatures
              .map(_.name)
              .toSeq
              .sorted
              .mkString(", ")
          )
        )

        sql(
          s"ALTER TABLE tbl " +
          s"SET TBLPROPERTIES('delta.feature.variantType-preview' = 'supported')"
        )
        sql("ALTER TABLE tbl ADD COLUMN v VARIANT")
        sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
        assert(spark.table("tbl").selectExpr("v::int").head == Row(99))

        assertVariantTypeTableFeatures(
          "tbl",
          expectPreviewFeature = true,
          expectStableFeature = false
        )
      }
    }
  }

  test("enabling 'FORCE_USE_PREVIEW_VARIANT_FEATURE' on table with stable feature does not " +
       "require adding preview feature") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assertVariantTypeTableFeatures(
        "tbl", expectPreviewFeature = false, expectStableFeature = true)

      withSQLConf(DeltaSQLConf.FORCE_USE_PREVIEW_VARIANT_FEATURE.key -> "true") {
        sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
        assert(spark.table("tbl").selectExpr("v::int").count == 2)
        assertVariantTypeTableFeatures(
          "tbl", expectPreviewFeature = false, expectStableFeature = true)
      }
    }
  }

  test("VariantType may not be used as a partition column") {
    withTable("delta_test") {
      checkError(
        intercept[AnalysisException] {
          sql(
            """CREATE TABLE delta_test(s STRING, v VARIANT)
              |USING delta
              |PARTITIONED BY (v)""".stripMargin)
        },
        "INVALID_PARTITION_COLUMN_DATA_TYPE",
        parameters = Map("type" -> "\"VARIANT\"")
      )
    }
  }

  test("streaming variant delta table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(100)
        .selectExpr("parse_json(cast(id as string)) v")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val streamingDf = spark.readStream
        .format("delta")
        .load(path)
        .selectExpr("v::int as extractedVal")

      val q = streamingDf.writeStream
        .format("memory")
        .queryName("test_table")
        .start()

      q.processAllAvailable()
      q.stop()

      val actual = spark.sql("select extractedVal from test_table")
      val expected = spark.sql("select id from range(100)")
      checkAnswer(actual, expected.collect())
    }
  }

  test("variant works with schema evolution for INSERT") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100, 1, 1)
        .selectExpr("id", "parse_json(cast(id as string)) v")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      spark.range(100, 200, 1, 1)
        .selectExpr(
          "id",
          "parse_json(cast(id as string)) v",
          "parse_json(cast(id as string)) v_two"
        )
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)

      val expected = spark.range(0, 200, 1, 1).selectExpr(
        "id",
        "parse_json(cast(id as string)) v",
        "case when id >= 100 then parse_json(cast(id as string)) else null end v_two"
      )

      val read = spark.read.format("delta").load(path)
      checkAnswer(read, expected.collect())
    }
  }

  test("variant works with schema evolution for MERGE") {
    withTempDir { dir =>
      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        val path = dir.getAbsolutePath
        spark.range(0, 100, 1, 1)
          .selectExpr("id", "parse_json(cast(id as string)) v")
          .write
          .format("delta")
          .mode("overwrite")
          .save(path)

        val sourceDf = spark.range(50, 200, 1, 1)
          .selectExpr(
            "id",
            "parse_json(cast(id as string)) v",
            "parse_json(cast(id as string)) v_two"
          )

        DeltaTable.forPath(spark, path)
          .as("source")
          .merge(sourceDf.as("target"), "source.id = target.id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()

        val expected = spark.range(0, 200, 1, 1).selectExpr(
          "id",
          "parse_json(cast(id as string)) v",
          "case when id >= 50 then parse_json(cast(id as string)) else null end v_two"
        )

        val read = spark.read.format("delta").load(path)
        checkAnswer(read, expected.collect())
      }
    }
  }

  test("variant cannot be used as a clustering column") {
    withTable("tbl") {
      val e = intercept[DeltaAnalysisException] {
        sql("CREATE TABLE tbl(v variant) USING DELTA CLUSTER BY (v)")
      }
      checkError(
        e,
        "DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED",
        parameters = Map("columnsWithDataTypes" -> "v : VARIANT")
      )
    }
  }

  test("describe history works with variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      // Create and insert should result in two table versions.
      assert(sql("DESCRIBE HISTORY tbl").count() == 2)
    }
  }

  test("describe detail works with variant column") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")

      val tableFeatures = sql("DESCRIBE DETAIL tbl")
        .selectExpr("tableFeatures")
        .collect()(0)
        .getAs[MutableSeq[String]](0)
      assert(tableFeatures.find(f => f == VariantTypePreviewTableFeature.name).isEmpty)
      assert(tableFeatures.find(f => f == VariantTypeTableFeature.name).nonEmpty)
    }
  }

  test("time travel with variant column works") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val initialDf = spark.range(0, 100, 1, 1).selectExpr("parse_json(cast(id as string)) v")
      initialDf
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      spark.range(100, 150, 1, 1).selectExpr("parse_json(cast(id as string)) v")
        .write
        .format("delta")
        .mode("append")
        .save(path)

      val timeTravelDf = spark.read.format("delta").option("versionAsOf", "0").load(path)
      checkAnswer(timeTravelDf, initialDf.collect())
    }
  }

  statsTest("optimize variant") {
    withTable("tbl") {
      spark.range(0, 100)
        .selectExpr("case when id % 2 = 0 then parse_json(cast(id as string)) else null end as v")
        .repartition(100)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("tbl")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val res = sql("OPTIMIZE tbl")
      val metrics = res.select($"metrics.*").as[OptimizeMetrics].head()
      assert(metrics.numFilesAdded > 0)
      assert(metrics.numTableColumnsWithStats == 1)

      val statsDf = getStatsDf(deltaLog, Seq($"numRecords", $"nullCount"))
      checkAnswer(statsDf, Row(100, Row(50)))
    }
  }

  test("Zorder is not supported for Variant") {
    withTable("tbl") {
      sql("CREATE TABLE tbl USING DELTA AS SELECT id, cast(null as variant) v from range(100)")
      val e = intercept[SparkException](sql("optimize tbl zorder by (v)"))
      checkError(
        e.getCause.asInstanceOf[SparkThrowable],
        "DATATYPE_MISMATCH.TYPE_CHECK_FAILURE_WITH_HINT",
        parameters = Map(
          "msg" -> "cannot sort data type variant",
          "hint" -> "",
          "sqlExpr" -> "\"rangepartitionid(v)\""))
    }
  }

  test("Table with variant type can use CDF") {
    withTable("tbl") {
      sql("""CREATE TABLE tbl USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
          AS SELECT parse_json(cast(id as string)) v from range(100)""")

      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) as v from range(0, 1))")
      sql("DELETE FROM tbl WHERE v::int = 0")
      sql("UPDATE tbl SET v = parse_json('-2') WHERE v::int = 50")

      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 0)
               where _change_type = 'delete'"""),
        Seq(Row("delete", 0), Row("delete", 0))
      )
      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 0)
               where _change_type = 'update_preimage'"""),
        Seq(Row("update_preimage", 50))
    )
      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 0)
               where _change_type = 'update_postimage'"""),
        Seq(Row("update_postimage", -2))
      )
    }
  }

  test("Existing table with variant type can enable CDF") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(v variant) USING DELTA")
      sql("ALTER TABLE tbl SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) as v from range(0, 100))")
      sql("DELETE FROM tbl WHERE v::string = '0'")
      sql("UPDATE tbl SET v = parse_json('-2') WHERE v::int = 50")

      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 1)
               where _change_type = 'delete'"""),
        Seq(Row("delete", 0))
      )
      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 1)
               where _change_type = 'update_preimage'"""),
        Seq(Row("update_preimage", 50))
      )
      checkAnswer(
        sql("""select _change_type, v::int from table_changes('tbl', 1)
               where _change_type = 'update_postimage'"""),
        Seq(Row("update_postimage", -2))
      )
    }
  }

  test(s"shallow cloning table with variant") {
    withTable("tbl", "clone_tbl") {
      sql("""CREATE TABLE tbl USING DELTA AS
          SELECT parse_json(cast(id as string)) v FROM range(100)""")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) as v from range(0, 10))")

      sql(s"CREATE TABLE IF NOT EXISTS clone_tbl SHALLOW CLONE tbl")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) as v from range(0, 10))")
      sql("INSERT INTO clone_tbl (SELECT parse_json(cast(id as string)) as v from range(0, 10))")

      val origTable = spark.sql("select * from tbl")
      val clonedTable = spark.sql("select * from clone_tbl")
      checkAnswer(clonedTable, origTable.collect())
    }
  }

  Seq("", "NO STATISTICS").foreach { statsClause =>
    test(s"Convert to Delta from parquet - $statsClause") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath

        spark.range(0, 100).selectExpr("parse_json(cast(id as string)) as v")
          .write
          .format("parquet")
          .mode("overwrite")
          .save(path)

        sql(s"CONVERT TO DELTA parquet.`$path` $statsClause")
        // Ensure Delta feature like column renaming works.
        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        sql(s"ALTER TABLE delta.`$path` RENAME COLUMN v TO new_v")

        val expectedDf = spark.range(0, 100).selectExpr("parse_json(cast(id as string)) as new_v")
        val actualDf = spark.sql(s"select * from delta.`$path`")
        checkAnswer(actualDf, expectedDf.collect())
      }
    }
  }

  Seq("name", "id").foreach { mode =>
    test(s"column mapping works - $mode") {
      withTable("tbl") {
        sql(s"""CREATE TABLE tbl USING DELTA
            TBLPROPERTIES ('delta.columnMapping.mode' = '$mode')
            AS SELECT parse_json(cast(id as string)) v, parse_json(cast(id as string)) v_two
            FROM range(5)""")
        val expectedAnswer = spark.sql("select v from tbl").collect()

        sql("ALTER TABLE tbl RENAME COLUMN v TO new_v")
        checkAnswer(spark.sql("select new_v from tbl"), expectedAnswer)

        sql("ALTER TABLE tbl DROP COLUMN new_v")
        // 'SELECT *' from the test table should return the same as `expectedAnswer` because `v` and
        // `v_two` are initially identical and `v` is dropped, resulting in a single column.
        checkAnswer(spark.sql("select * from tbl"), expectedAnswer)
      }
    }
  }

  test("Variant can have default value set") {
    withTable("tbl") {
      sql("""CREATE TABLE tbl USING DELTA
          TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
          AS SELECT parse_json(cast(id as string)) v from range(5)""")

      sql("INSERT INTO tbl VALUES (DEFAULT)")
      val nullCount = spark.sql("SELECT * FROM tbl WHERE v is null").count()
      // Default DEFAULT value is null.
      assert(nullCount == 1)

      sql("ALTER TABLE tbl ALTER COLUMN v SET DEFAULT (parse_json('{\"k\": \"v\"}'))")
      sql("INSERT INTO tbl VALUES (DEFAULT)")
      checkAnswer(
        sql("SELECT v FROM tbl WHERE variant_get(v, '$.k', 'STRING') = 'v'"),
        sql("select parse_json('{\"k\": \"v\"}')").collect
      )
    }
  }

  test("Variant can be used as a source for generated columns") {
    withTable("tbl") {
      DeltaTable.create(spark)
        .tableName("tbl")
        .addColumn("v", "VARIANT")
        .addColumn(
          DeltaTable.columnBuilder(spark, "vInt")
            .dataType("INT")
            .generatedAlwaysAs("v::int")
            .build()
        )
        .execute()
      spark.range(0, 100)
        .selectExpr("parse_json(cast(id as string)) as v")
        .write
        .mode("append")
        .format("delta")
        .saveAsTable("tbl")
      val expectedDf = spark.range(0, 100).selectExpr(
        "parse_json(cast(id as string)) v",
        "cast(id as int) vInt"
      )
      val actualDf = spark.sql("select * from tbl")
      checkAnswer(actualDf, expectedDf.collect())
    }
  }

  test("Variant cannot be created as a generated column") {
    withTable("tbl") {
      val e = intercept[DeltaAnalysisException] {
        DeltaTable.create(spark)
          .tableName("tbl")
          .addColumn("id", "INT")
          .addColumn(
            DeltaTable.columnBuilder(spark, "v")
              .dataType("VARIANT")
              .generatedAlwaysAs("parse_json(cast(id as string))")
              .build()
          )
          .execute()
      }
      checkError(
        e,
        "DELTA_UNSUPPORTED_DATA_TYPE_IN_GENERATED_COLUMN",
        parameters = Map("dataType" -> "VARIANT")
      )
    }
  }

  test("Variant respects Delta table IS NOT NULL constraints") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(v variant NOT NULL) USING DELTA")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) from range(0, 100))")
      val insertException = intercept[DeltaInvariantViolationException] {
        sql("INSERT INTO tbl VALUES (cast(null as variant))")
      }
      checkError(
        insertException,
        "DELTA_NOT_NULL_CONSTRAINT_VIOLATED",
        parameters = Map("columnName" -> "v")
      )

      sql("ALTER TABLE tbl ALTER COLUMN v DROP NOT NULL")
      // Inserting null value should work now.
      sql("INSERT INTO tbl VALUES (cast(null as variant))")
      val nullCount = spark.sql("select * from tbl where v is null").count()
      assert(nullCount == 1)
    }
  }

  test("Variant respects Delta table CHECK constraints") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(v variant) USING DELTA")
      sql("ALTER TABLE tbl ADD CONSTRAINT variantGTEZero CHECK (variant_get(v, '$', 'INT') >= 0)")
      sql("INSERT INTO tbl (SELECT parse_json(cast(id as string)) from range(0, 100))")

      val insertException = intercept[DeltaInvariantViolationException] {
        sql("INSERT INTO tbl (select parse_json(cast(id as string)) from range(-1, 0))")
      }
      checkError(
        insertException,
        "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
        parameters = Map(
          "constraintName" -> "variantgtezero",
          "expression" -> "(variant_get(v, '$', 'INT') >= 0)", "values" -> " - v : -1"
        )
      )

      sql("ALTER TABLE tbl DROP CONSTRAINT variantGTEZero")
      sql("INSERT INTO tbl (select parse_json(cast(id as string)) from range(-1, 0))")
      val lessThanZeroCount = spark.sql("select * from tbl where v::int < 0").count()
      // Inserting variant with value less than zero should work after dropping constraint.
      assert(lessThanZeroCount == 1)
      val addConstraintException = intercept[DeltaAnalysisException] {
        sql("ALTER TABLE tbl ADD CONSTRAINT variantGTEZero CHECK (variant_get(v, '$', 'INT') >= 0)")
      }
      checkError(
        addConstraintException,
        "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
        parameters = Map(
          "numRows" -> "1",
          "tableName" -> "spark_catalog.default.tbl",
          "checkConstraint" -> "variant_get ( v , '$' , 'INT' ) >= 0"
        )
      )

      sql("DELETE FROM tbl WHERE variant_get(v, '$', 'INT') < 0")
      // Adding the constraint should work after deleting the variant that is less than zero.
      sql("ALTER TABLE tbl ADD CONSTRAINT variantGTEZero CHECK (variant_get(v, '$', 'INT') >= 0)")
      val newLessThanZeroCount = spark.sql("select * from tbl where v::int < 0").count()
      assert(newLessThanZeroCount == 0)
    }
  }
}
