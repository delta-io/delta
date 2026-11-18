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

package org.apache.spark.sql.delta.files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

class TransactionalWriteSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("writing out an empty dataframe produces no AddFiles") {
    withTempDir { dir =>
      spark.range(100).write.format("delta").save(dir.getCanonicalPath)

      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val schema = new StructType().add("id", StringType)
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      assert(log.startTransaction().writeFiles(emptyDf).isEmpty)
    }
  }

  test("write data files to the data subdir") {
    withSQLConf(DeltaSQLConf.WRITE_DATA_FILES_TO_SUBDIR.key -> "true") {
      def validateDataSubdir(tablePath: String): Unit = {
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tablePath)
        snapshot.allFiles.collect().foreach { f =>
          assert(f.path.startsWith("data/"))
        }
      }

      withTempDir { dir =>
        spark.range(100).toDF("id").write.format("delta").save(dir.getCanonicalPath)
        validateDataSubdir(dir.getCanonicalPath)
      }

      withTempDir { dir =>
        spark.range(100).toDF("id").withColumn("id1", column("id")).write.format("delta")
          .partitionBy("id").save(dir.getCanonicalPath)
        validateDataSubdir(dir.getCanonicalPath)
      }
    }

    withSQLConf(DeltaSQLConf.WRITE_DATA_FILES_TO_SUBDIR.key -> "false") {
      withTempDir { dir =>
        spark.range(100).toDF("id").write.format("delta").save(dir.getCanonicalPath)
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, dir.getCanonicalPath)
        snapshot.allFiles.collect().foreach { f =>
          assert(!f.path.startsWith("data/"))
        }
      }
    }
  }

  test("partitioned table defaults to writing partition columns to parquet files") {
    withTempDir { dir =>
      spark.range(100).toDF("id")
        .withColumn("partCol", column("id") % 5)
        .write.format("delta")
        .partitionBy("partCol")
        .save(dir.getCanonicalPath)

      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val snapshot = deltaLog.update()
      val files = snapshot.allFiles.collect()
      assert(files.nonEmpty)

      val logicalToPhysicalNameMap =
        DeltaColumnMapping.getLogicalNameToPhysicalNameMap(snapshot.schema)
      val physicalPartCol = logicalToPhysicalNameMap(Seq("partCol")).head

      files.foreach { file =>
        val filePath = DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, file.path)
        val fileReader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filePath.toString), new Configuration()))
        val parquetSchema = try {
          fileReader.getFooter.getFileMetaData.getSchema
        } finally {
          fileReader.close()
        }
        val fieldNames = parquetSchema.getFields.asScala.map(_.getName).toSet
        assert(fieldNames.contains(physicalPartCol),
          s"Partition column 'partCol' (physical: '$physicalPartCol') should be present " +
            s"in parquet file ${file.path}, but found fields: $fieldNames")
      }
    }
  }

  /** A way of writing the rows of [[NestedNullsWrite.query]] into the table at a path. */
  private case class NestedNullsWrite(name: String, write: String => Unit)

  private object NestedNullsWrite {
    // lag and lead take their type from the input expression only, so the query reports the map
    // values, the inner array elements and the values nested in a map value as NOT NULL,
    // although the defaults put NULLs there for id 0.
    val query: String =
      """SELECT id,
        |  lag(map('k', v), 1, map('k', w)) OVER (ORDER BY id) AS m,
        |  lag(array(array(v)), 1, array(array(w))) OVER (ORDER BY id) AS a,
        |  lead(map('k', array(v)), 1, map('k', array(w))) OVER (ORDER BY id DESC) AS ma,
        |  lead(map('k', map('j', v)), 1, map('k', map('j', w))) OVER (ORDER BY id DESC) AS mm
        |FROM (
        |  SELECT CAST(id AS INT) AS id, CAST(id AS INT) AS v,
        |    IF(id = 0, NULL, CAST(id AS INT)) AS w
        |  FROM range(3))
        |""".stripMargin

    /** The rows of [[readValues]] after writing [[query]]. */
    val expectedRows: Seq[Row] = Seq(
      Row(0, null, null, null, null),
      Row(1, 0, 0, 0, 0),
      Row(2, 1, 1, 1, 1))

    /** Reads the nested values of the table at a path. */
    def readValues(path: String): DataFrame =
      spark.read.format("delta").load(path)
        .selectExpr("id", "m['k']", "a[0][0]", "ma['k'][0]", "mm['k']['j']")

    private val tableSchema = "id INT, m MAP<STRING, INT>, a ARRAY<ARRAY<INT>>, " +
      "ma MAP<STRING, ARRAY<INT>>, mm MAP<STRING, MAP<STRING, INT>>"

    private def createEmptyTable(path: String): Unit =
      sql(s"CREATE TABLE delta.`$path` ($tableSchema) USING delta")

    /** Creates a table with the ids of the query and placeholder values without NULLs. */
    private def createSeededTable(path: String): Unit = {
      createEmptyTable(path)
      sql(s"INSERT INTO delta.`$path` SELECT id, map('k', -1), array(array(-1)), " +
        "map('k', array(-1)), map('k', map('j', -1)) FROM range(3)")
    }

    /** The ways of writing [[query]] that the tests cover. */
    val all: Seq[NestedNullsWrite] = Seq(
      NestedNullsWrite("CREATE TABLE AS SELECT", path =>
        sql(s"CREATE TABLE delta.`$path` USING delta AS $query")),
      NestedNullsWrite("REPLACE TABLE AS SELECT", { path =>
        createEmptyTable(path)
        sql(s"REPLACE TABLE delta.`$path` USING delta AS $query")
      }),
      NestedNullsWrite("CREATE OR REPLACE TABLE AS SELECT", { path =>
        createEmptyTable(path)
        sql(s"CREATE OR REPLACE TABLE delta.`$path` USING delta AS $query")
      }),
      NestedNullsWrite("INSERT INTO", { path =>
        createEmptyTable(path)
        sql(s"INSERT INTO delta.`$path` $query")
      }),
      NestedNullsWrite("INSERT OVERWRITE", { path =>
        createSeededTable(path)
        sql(s"INSERT OVERWRITE delta.`$path` $query")
      }),
      NestedNullsWrite("DataFrame save", path =>
        sql(query).write.format("delta").save(path)),
      NestedNullsWrite("DataFrame append", { path =>
        createEmptyTable(path)
        sql(query).write.format("delta").mode("append").save(path)
      }),
      NestedNullsWrite("DataFrame overwrite", { path =>
        createSeededTable(path)
        sql(query).write.format("delta").mode("overwrite").save(path)
      }),
      NestedNullsWrite("DataFrameWriterV2 append", { path =>
        createEmptyTable(path)
        sql(query).writeTo(s"delta.`$path`").append()
      }),
      NestedNullsWrite("MERGE INSERT", { path =>
        createEmptyTable(path)
        sql(s"MERGE INTO delta.`$path` t USING ($query) s ON t.id = s.id " +
          "WHEN NOT MATCHED THEN INSERT *")
      }),
      NestedNullsWrite("INSERT INTO BY NAME", { path =>
        createEmptyTable(path)
        sql(s"INSERT INTO delta.`$path` BY NAME $query")
      }),
      NestedNullsWrite("MERGE UPDATE SET *", { path =>
        createSeededTable(path)
        sql(s"MERGE INTO delta.`$path` t USING ($query) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET *")
      }),
      NestedNullsWrite("MERGE UPDATE SET columns", { path =>
        createSeededTable(path)
        sql(s"MERGE INTO delta.`$path` t USING ($query) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET m = s.m, a = s.a, ma = s.ma, mm = s.mm")
      }),
      NestedNullsWrite("UPDATE with scalar subqueries", { path =>
        createSeededTable(path)
        for (id <- 0 until 3) {
          val row = s"FROM ($query) WHERE id = $id"
          sql(s"UPDATE delta.`$path` SET m = (SELECT m $row), a = (SELECT a $row), " +
            s"ma = (SELECT ma $row), mm = (SELECT mm $row) WHERE id = $id")
        }
      })
    )
  }

  // The following tests verify that Delta defends against misbehaving Spark operators that
  // produce NULLs but claim to return a NOT NULL data type. This test ensures that the query that
  // generates the data to write reproduces this incorrect behavior.
  test("ensure NestedNullsWrite.query produces NULLs but returns a NOT NULL data type") {
    val df = sql(NestedNullsWrite.query)
    val m = df.schema("m").dataType.asInstanceOf[MapType]
    val a = df.schema("a").dataType.asInstanceOf[ArrayType]
    val ma = df.schema("ma").dataType.asInstanceOf[MapType]
    val mm = df.schema("mm").dataType.asInstanceOf[MapType]
    if (m.valueContainsNull ||
        a.elementType.asInstanceOf[ArrayType].containsNull ||
        ma.valueContainsNull || ma.valueType.asInstanceOf[ArrayType].containsNull ||
        mm.valueContainsNull || mm.valueType.asInstanceOf[MapType].valueContainsNull) {
      fail(
        """Ooopsie! The tests in this suite that write NestedNullsWrite.query depend on a defect
          |of the lag and lead window functions: with a NULL default, they produce NULLs in nested
          |fields that their result type reports as NOT NULL. That defect appears to be at least
          |partially fixed.
          |
          |UNPIVOT has it too, with spark.sql.legacy.unpivot.useFirstValueNestedNullability=true.
          |For example, with that conf this query reports m as MAP<STRING, INT NOT NULL> although
          |one of the values it unpivots is NULL:
          |
          |  SELECT m FROM (SELECT map('k', 1) AS m1, map('k', CAST(NULL AS INT)) AS m2)
          |  UNPIVOT INCLUDE NULLS (m FOR c IN (m1, m2))
          |
          |Rewrite NestedNullsWrite.query with UNPIVOT and that conf. If that doesn't work either,
          |find another query with this defect. If there is none, disable the test cases that
          |depend on it.
          |""".stripMargin)
    }
    // The values are NULL nonetheless. to_json checks for NULLs whatever the reported nullability.
    checkAnswer(
      df.selectExpr("id", "to_json(m)", "to_json(a)", "to_json(ma)", "to_json(mm)"),
      Seq(
        Row(0, """{"k":null}""", "[[null]]", """{"k":[null]}""", """{"k":{"j":null}}"""),
        Row(1, """{"k":0}""", "[[0]]", """{"k":[0]}""", """{"k":{"j":0}}"""),
        Row(2, """{"k":1}""", "[[1]]", """{"k":[1]}""", """{"k":{"j":1}}""")))
  }

  for (write <- NestedNullsWrite.all) {
    test("NULLs in nested fields that the query reports as NOT NULL are readable after " +
        write.name) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        write.write(path)
        checkAnswer(NestedNullsWrite.readValues(path), NestedNullsWrite.expectedRows)
      }
    }
  }
}
