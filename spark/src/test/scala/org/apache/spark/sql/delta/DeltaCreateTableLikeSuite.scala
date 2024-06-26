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

import java.io.File
import java.net.URI
import java.util.UUID

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{CreateDeltaTableCommand, TableCreationModes}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaCreateTableLikeSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  def checkTableEmpty(tblName: String): Boolean = {
    val numRows = spark.sql(s"SELECT * FROM $tblName")
    numRows.count() == 0
  }

  /**
   * This method checks if certain properties and fields of delta tables are the
   * same between the two delta tables. Boolean values can be passed in to check
   * or not to check (assert) the specific property. Note that for checkLocation
   * a boolean value is not passed in. If checkLocation argument is None, location
   * of target table will not be checked.
   *
   * @param checkTargetTableByPath when true, targetTbl must be a path not table name
   * @param checkSourceTableByPath when true, srcTbl must be a path not table name
   */
  def checkTableCopyDelta(
      srcTbl: String,
      targetTbl: String,
      checkDesc: Boolean = true,
      checkSchemaString: Boolean = true,
      checkPartitionColumns: Boolean = true,
      checkConfiguration: Boolean = true,
      checkTargetTableByPath: Boolean = false,
      checkSourceTableByPath: Boolean = false,
      checkLocation: Option[String] = None): Unit = {
    val src =
      if (checkSourceTableByPath) {
        DeltaLog.forTable(spark, srcTbl)
      } else {
        DeltaLog.forTable(spark, TableIdentifier(srcTbl))
      }

    val target =
      if (checkTargetTableByPath) {
        DeltaLog.forTable(spark, targetTbl)
      } else {
        DeltaLog.forTable(spark, TableIdentifier(targetTbl))
      }
    assert(src.unsafeVolatileSnapshot.protocol ==
           target.unsafeVolatileSnapshot.protocol,
          "protocol does not match")
    if (checkDesc) {
      assert(src.unsafeVolatileSnapshot.metadata.description ==
        target.unsafeVolatileSnapshot.metadata.description,
        "description/comment does not match")
    }
    if (checkSchemaString) {
      assert(src.unsafeVolatileSnapshot.metadata.schemaString ==
        target.unsafeVolatileSnapshot.metadata.schemaString,
        "schema does not match")
    }
    if (checkPartitionColumns) {
      assert(src.unsafeVolatileSnapshot.metadata.partitionColumns ==
        target.unsafeVolatileSnapshot.metadata.partitionColumns,
      "partition columns do not match")
    }
    if (checkConfiguration) {
      // Checks Table properties and table constraints
      assert(src.unsafeVolatileSnapshot.metadata.configuration ==
        target.unsafeVolatileSnapshot.metadata.configuration,
      "configuration does not match")
    }

    val catalog = spark.sessionState.catalog
    if(checkLocation.isDefined) {
      assert(
        catalog.getTableMetadata(TableIdentifier(targetTbl)).location.toString + "/"
          == checkLocation.get ||
          catalog.getTableMetadata(TableIdentifier(targetTbl)).location.toString ==
            checkLocation.get, "location does not match")
    }

  }

  /**
   * This method checks if certain properties and fields of a table are the
   * same between two tables. Boolean values can be passed in to check
   * or not to check (assert) the specific property. Note that for checkLocation
   * a boolean value is not passed in. If checkLocation argument is None, location
   * of target table will not be checked.
   */
  def checkTableCopy(
      srcTbl: String, targetTbl: String,
      checkDesc: Boolean = true,
      checkSchemaString: Boolean = true,
      checkPartitionColumns: Boolean = true,
      checkConfiguration: Boolean = true,
      checkProvider: Boolean = true,
      checkLocation: Option[String] = None): Unit = {
    val srcTblDesc = spark.sessionState.catalog.
      getTempViewOrPermanentTableMetadata(TableIdentifier(srcTbl))
    val targetTblDesc = DeltaLog.forTable(spark, TableIdentifier(targetTbl))
    val targetTblMetadata = targetTblDesc.unsafeVolatileSnapshot.metadata
    if (checkDesc) {
      assert(srcTblDesc.comment == Some(targetTblMetadata.description),
        "description/comment does not match")
    }
    if (checkSchemaString) {
      assert(srcTblDesc.schema ==
        targetTblDesc.unsafeVolatileSnapshot.metadata.schema,
        "schema does not match")
    }
    if (checkPartitionColumns) {
      assert(srcTblDesc.partitionColumnNames ==
        targetTblMetadata.partitionColumns,
        "partition columns do not match")
    }
    if (checkConfiguration) {
      // Checks Table properties
      assert(srcTblDesc.properties == targetTblMetadata.configuration,
        "configuration does not match")
    }
    if (checkProvider) {
      val targetTblProvider = spark.sessionState.catalog.
        getTempViewOrPermanentTableMetadata(TableIdentifier(targetTbl)).provider
      assert(srcTblDesc.provider == targetTblProvider,
        "provider does not match")
    }
    val catalog = spark.sessionState.catalog
    if(checkLocation.isDefined) {
      assert(
        catalog.getTableMetadata(TableIdentifier(targetTbl)).location.toString + "/"
          == checkLocation.get ||
          catalog.getTableMetadata(TableIdentifier(targetTbl)).location.toString ==
            checkLocation.get)
    }
  }

  def createTable(
      srcTbl: String, format: String = "delta",
      addTblProperties: Boolean = true,
      addComment: Boolean = true): Unit = {
    spark.range(100)
      .withColumnRenamed("id", "key")
      .withColumn("newCol", lit(1))
      .write
      .format(format)
      .partitionBy("key")
      .saveAsTable(srcTbl)

    if (addTblProperties) {
      spark.sql(s"ALTER TABLE $srcTbl" +
        " SET TBLPROPERTIES(this.is.my.key = 14, 'this.is.my.key2' = false)")
    }
    if (format == "delta") {
      spark.sql(s"ALTER TABLE $srcTbl SET TBLPROPERTIES('delta.minReaderVersion' = '2'," +
        " 'delta.minWriterVersion' = '5')")
    }
    if (addComment) {
      spark.sql(s"COMMENT ON TABLE $srcTbl IS 'srcTbl'")
    }
  }

  test("CREATE TABLE LIKE basic test") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl)
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE with no comment") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl, addComment = false)
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE with no added table properties") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl, addTblProperties = false)
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE where table has no schema") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      spark.sql(s"CREATE TABLE $srcTbl USING DELTA")
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE with no added constraints") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl
      )
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE with IF NOT EXISTS, given that targetTable does not exist") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl)
      spark.sql(s"CREATE TABLE IF NOT EXISTS $targetTbl LIKE $srcTbl USING DELTA")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE with IF NOT EXISTS, given that targetTable does exist") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl)
      spark.sql(s"CREATE TABLE $targetTbl(key4 INT) USING DELTA")
      spark.sql(s"CREATE TABLE IF NOT EXISTS $targetTbl LIKE  $srcTbl")

      val msg = intercept[TestFailedException] {
        checkTableCopyDelta(srcTbl, targetTbl)
      }.getMessage
      assert(msg.contains("protocol does not match"))
    }
  }

  test("CREATE TABLE LIKE without IF NOT EXISTS, given that targetTable does exist") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl)
      spark.range(100).repartition(3)
        .withColumnRenamed("id4", "key4")
        .write
        .format("delta")
        .saveAsTable(targetTbl)

      val msg = intercept[DeltaAnalysisException] {
        spark.sql(s"CREATE TABLE $targetTbl LIKE  $srcTbl")
      }.getMessage
      msg.contains("Table `default`.`targetTbl` already exists.")
    }
  }

  test("concurrent create Managed Catalog table commands should not fail") {
    withTempDir { dir =>
      withTable("t") {
        def getCatalogTable: CatalogTable = {
          val storage = CatalogStorageFormat.empty.copy(
            locationUri = Some(new URI(s"$dir/${UUID.randomUUID().toString}")))
          val catalogTableTarget = CatalogTable(
            identifier = TableIdentifier("t"),
            tableType = CatalogTableType.MANAGED,
            storage = storage,
            provider = Some("delta"),
            schema = new StructType().add("id", "long"))
          new DeltaCatalog()
            .verifyTableAndSolidify(
              tableDesc = catalogTableTarget,
              query = None,
              maybeClusterBySpec = None)
        }
        CreateDeltaTableCommand(
          getCatalogTable,
          existingTableOpt = None,
          mode = SaveMode.Ignore,
          query = None,
          operation = TableCreationModes.Create).run(spark)
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
        CreateDeltaTableCommand(
          getCatalogTable,
          existingTableOpt = None, // Set to None to simulate concurrent table creation commands.
          mode = SaveMode.Ignore,
          query = None,
          operation = TableCreationModes.Create).run(spark)
        assert(spark.sessionState.catalog.tableExists(TableIdentifier("t")))
      }
    }
  }

  test("CREATE TABLE LIKE where sourceTable is a json table") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl, format = "json")
      spark.sql(s"CREATE TABLE $targetTbl LIKE  $srcTbl USING DELTA")
      // Provider should be different, expected exception to be thrown
      val msg = intercept[TestFailedException] {
        checkTableCopy(srcTbl, targetTbl, checkDesc = false)
      }.getMessage
      assert(msg.contains("provider does not match"))
    }
  }

  test("CREATE TABLE LIKE where sourceTable is a parquet table") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl, format = "parquet")
      spark.sql(s"CREATE TABLE $targetTbl LIKE  $srcTbl USING DELTA")
      // Provider should be different, expected exception to be thrown
      val msg = intercept[TestFailedException] {
        checkTableCopy(srcTbl, targetTbl, checkDesc = false)
      }.getMessage
      assert(msg.contains("provider does not match"))
    }
  }

  test("CREATE TABLE LIKE test where source table is an external table") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTempDir { dir =>
      val path = dir.toURI.toString

      new File(dir.getAbsolutePath, srcTbl).mkdir()
      withTable(srcTbl, targetTbl) {
        spark.sql(s"CREATE TABLE $srcTbl (key STRING) USING DELTA LOCATION '$path/$srcTbl'")
        spark.sql(s"ALTER TABLE $srcTbl" +
          s" SET TBLPROPERTIES(this.is.my.key = 14, 'this.is.my.key2' = false)")
        spark.sql(s"COMMENT ON TABLE $srcTbl IS 'srcTbl'")
        spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl")

        checkTableCopyDelta(srcTbl, targetTbl)
      }
    }
  }

  test("CREATE TABLE LIKE where target table is a named external table") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTempDir { dir =>
      withTable(srcTbl) {
        createTable(srcTbl)
        spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl LOCATION '${dir.toURI.toString}'")
        checkTableCopyDelta(srcTbl, targetTbl, checkLocation = Some(dir.toURI.toString))
      }
    }
  }

  test("CREATE TABLE LIKE where target table is a nameless table") {
    val srcTbl = "srcTbl"
    withTempDir { dir =>
      withTable(srcTbl) {
        createTable(srcTbl)
        spark.sql(s"CREATE TABLE delta.`${dir.toURI.toString}` LIKE $srcTbl")
        checkTableCopyDelta(srcTbl, dir.toString, checkTargetTableByPath = true
        )
      }
    }
  }

  test("CREATE TABLE LIKE where source is a view") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    val srcView = "srcView"
    withTable(srcTbl, targetTbl) {
      withView(srcView) {
        createTable(srcTbl)
        spark.sql(s"DROP TABLE IF EXISTS $targetTbl")
        spark.sql(s"CREATE VIEW srcView AS SELECT * FROM $srcTbl")
        spark.sql(s"CREATE TABLE $targetTbl LIKE $srcView USING DELTA")
        val targetTableDesc = DeltaLog.forTable(spark, TableIdentifier(targetTbl))
        val srcViewDesc = spark.sessionState.catalog.
          getTempViewOrPermanentTableMetadata(TableIdentifier(srcView))
        assert(targetTableDesc.unsafeVolatileSnapshot.metadata.schema == srcViewDesc.schema)
      }
    }
  }

  test("CREATE TABLE LIKE where source is a temporary view") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    val srcView = "srcView"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl)
      spark.sql(s"CREATE TEMPORARY VIEW srcView AS SELECT * FROM $srcTbl")
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcView USING DELTA")
      val targetTableDesc = DeltaLog.forTable(spark, TableIdentifier(targetTbl))
      val srcViewDesc = spark.sessionState.catalog.
        getTempViewOrPermanentTableMetadata(TableIdentifier(srcView))
      assert(targetTableDesc.unsafeVolatileSnapshot.metadata.schema == srcViewDesc.schema)
    }
  }

  test("CREATE TABLE LIKE where source table has a column mapping") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl
      )
      // Need to set minWriterVersion to 5 for column mappings to work
      spark.sql(s"ALTER TABLE $srcTbl SET TBLPROPERTIES('delta.minReaderVersion' = '2'," +
        " 'delta.minWriterVersion' = '5')")
      // Need to set delta.columnMapping.mode to 'name' for column mappings to work
      spark.sql(s"ALTER TABLE $srcTbl SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      spark.sql(s"ALTER TABLE $srcTbl RENAME COLUMN key TO key2")
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl USING DELTA")
      checkTableCopyDelta(srcTbl, targetTbl)
    }
  }

  test("CREATE TABLE LIKE where user explicitly provides table properties") {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    val expectedTbl = "expectedTbl"
    withTable(srcTbl, targetTbl, expectedTbl) {
      createTable(srcTbl, addTblProperties = false)
      createTable(expectedTbl, addTblProperties = false)
      spark.sql(s"ALTER TABLE $srcTbl" +
        " SET TBLPROPERTIES(this.is.my.key = 14, 'this.is.my.key2' = false," +
        "'delta.appendOnly' = 'false')")
      spark.sql(s"ALTER TABLE $expectedTbl" +
        " SET TBLPROPERTIES(this.is.my.key = 14, 'this.is.my.key2' = false, " +
        "'this.is.my.key3' = true, 'delta.appendOnly' = 'true')")
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl TBLPROPERTIES('this.is.my.key3' = true, " +
        s"'delta.appendOnly' = 'true')")
      checkTableCopyDelta(expectedTbl, targetTbl)
    }
  }

    test("CREATE TABLE LIKE where sourceTable is a parquet table and " +
      "user explicitly provides table properties"
      ) {
    val srcTbl = "srcTbl"
    val targetTbl = "targetTbl"
    withTable(srcTbl, targetTbl) {
      createTable(srcTbl, format = "parquet")
      spark.sql(s"CREATE TABLE $targetTbl LIKE $srcTbl USING DELTA " +
        "TBLPROPERTIES('this.is.my.key3' = true)")
      spark.sql(s"ALTER TABLE $srcTbl SET TBLPROPERTIES('this.is.my.key3' = true)")
      val msg = intercept[TestFailedException] {
        checkTableCopy(srcTbl, targetTbl, checkDesc = false)
      }.getMessage
      assert(msg.contains("provider does not match"))
    }
  }
}
