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

package org.apache.spark.sql.delta.cdc

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class CDCNullabilitySuite extends QueryTest
  with SharedSparkSession
  with CDCEnabled
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with CDCTestMixin {

  protected def tableProvider: String = "delta"
  protected def writeFormat: String = tableProvider

  /** Tightens `columnPath` in a separate commit for `tableName`. */
  private def setNotNull(tableName: String, columnPath: Seq[String]): Unit = {
    // Delta does not support setting a column to NOT NULL, so commit the metadata change manually.
    // CDF must still support reading changes from a table where another writer changed a column's
    // nullability.
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    deltaLog.withNewTransaction { txn =>
      val metadata = txn.metadata
      val schema = SchemaMergingUtils.transformColumns(metadata.schema) { (path, field, _) =>
        if ((path :+ field.name) == columnPath) field.copy(nullable = false) else field
      }
      txn.commit(metadata.copy(schemaString = schema.json) :: Nil, DeltaOperations.ManualUpdate)
    }
  }

  test("CDF across SET NOT NULL schema changes fails") {
    withSQLConf(
        DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "true") {
      val tblName = "customers"
      val otherTbl = "accounts"
      withTable(tblName, otherTbl) {
        sql(s"""CREATE TABLE $tblName (id INT, email STRING, tier INT)
               |USING $tableProvider""".stripMargin)
        sql(s"INSERT INTO $tblName VALUES (1, NULL, 1), (2, 'b@x.com', 2), (3, NULL, NULL)")
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val insertVersion = deltaLog.update().version
        sql(s"UPDATE $tblName SET email = concat('user', id, '@x.com') WHERE email IS NULL")
        sql(s"UPDATE $tblName SET tier = 0 WHERE tier IS NULL")
        setNotNull(tblName, Seq("email"))
        setNotNull(tblName, Seq("tier"))
        val constrainedVersion = deltaLog.update().version

        sql(s"""CREATE TABLE $otherTbl (id INT NOT NULL, name STRING NOT NULL)
               |USING $tableProvider""".stripMargin)
        sql(s"INSERT INTO $otherTbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        val changes = s"table_changes('$tblName', $insertVersion)"
        val queries = Seq(
          s"SELECT id, email, _change_type FROM $changes",
          s"SELECT id, _change_type FROM $changes WHERE email IS NULL",
          s"SELECT count(*) FROM $changes WHERE email IS NULL AND _change_type = 'insert'",
          s"SELECT EXISTS (SELECT 1 FROM $changes WHERE tier IS NULL)",
          s"SELECT count(*) FROM $changes WHERE isnull(email) OR isnull(tier)",
          s"SELECT count(email) FROM $changes",
          s"SELECT (SELECT count(*) FROM $changes WHERE email IS NULL)",
          s"SELECT * FROM $otherTbl WHERE id IN (SELECT id FROM $changes WHERE email IS NULL)",
          s"""SELECT * FROM $otherTbl o WHERE EXISTS
             |(SELECT 1 FROM $changes c WHERE c.id = o.id AND c.email IS NULL)""".stripMargin,
          s"SELECT * FROM $otherTbl o JOIN $changes c ON o.id = c.id WHERE c.email IS NULL",
          s"""SELECT * FROM $otherTbl o LEFT JOIN $changes c
             |ON o.id = c.id AND c.email IS NULL""".stripMargin,
          s"""SELECT * FROM $otherTbl o JOIN $changes c ON o.id = c.id
             |WHERE c.email IS NULL OR o.name IS NULL""".stripMargin,
          s"""SELECT * FROM $otherTbl o JOIN $changes c
             |ON o.id = c.id AND (c.email IS NULL OR o.name IS NULL)""".stripMargin,
          s"""SELECT * FROM $otherTbl o LEFT JOIN $changes c
             |ON o.id = c.id AND (c.email IS NULL OR o.name IS NULL)""".stripMargin)
        queries.foreach { query =>
          withClue(s"Query: $query\n") {
            val e = intercept[DeltaUnsupportedOperationException] {
              sql(query).collect()
            }
            assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")
          }
        }

        // Verify behavior for the DataFrame API.
        val cdf = spark.read.format(writeFormat)
          .option(DeltaDataSource.CDC_ENABLED_KEY, "true")
          .option(DeltaDataSource.CDC_START_VERSION_KEY, insertVersion)
          .table(tblName)
        val e = intercept[DeltaUnsupportedOperationException] {
          cdf.filter(col("email").isNull).collect()
        }
        assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")

        // A range starting at the final constraint commit remains readable.
        sql(s"INSERT INTO $tblName VALUES (4, 'd@x.com', 4)")
        val validChanges = s"table_changes('$tblName', $constrainedVersion)"
        checkAnswer(sql(s"SELECT id, email, tier FROM $validChanges"), Row(4, "d@x.com", 4))
        checkAnswer(sql(s"SELECT count(*) FROM $validChanges WHERE email IS NULL"), Row(0L))
      }
    }
  }

  test("CDF filter pruning fix can be disabled") {
    withSQLConf(
        DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "false") {
      val tblName = "contacts"
      withTable(tblName) {
        sql(s"""CREATE TABLE $tblName
               |(id INT NOT NULL, email STRING, contact STRUCT<email: STRING> NOT NULL)
               |USING $tableProvider""".stripMargin)
        sql(s"INSERT INTO $tblName VALUES (1, NULL, named_struct('email', NULL))")
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val startVersion = deltaLog.update().version
        sql(s"UPDATE $tblName SET email = 'a@x.com', contact.email = 'a@x.com'")
        setNotNull(tblName, Seq("email"))
        setNotNull(tblName, Seq("contact", "email"))
        val endVersion = deltaLog.update().version
        val cdf = sql(s"SELECT * FROM table_changes('$tblName', $startVersion, $endVersion)")

        // An unfiltered query still detects the incompatible range with the fix disabled.
        val e = intercept[DeltaUnsupportedOperationException] {
          cdf.collect()
        }
        assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")

        // A filtered query succeeds with an incorrect empty result.
        Seq("email", "contact.email").foreach { column =>
          val filteredCDF = cdf.filter(col(column).isNull)
          checkAnswer(filteredCDF, Seq.empty[Row])

          // Reanalyze the same query after enabling the fix: it must now validate the range.
          // This check also validates that we correctly catch nested nullability.
          withSQLConf(DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "true") {
            val e = intercept[DeltaUnsupportedOperationException] {
              sql(s"""SELECT * FROM table_changes('$tblName', $startVersion, $endVersion)
                     |WHERE $column IS NULL""".stripMargin).collect()
            }
            assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")
          }
        }
      }
    }
  }

  test("CDF nullability reversals between start and end versions can still bypass validation") {
    withSQLConf(
        DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "true") {
      val tblName = "contacts"
      withTable(tblName) {
        sql(s"""CREATE TABLE $tblName (id INT NOT NULL, email STRING NOT NULL)
               |USING $tableProvider""".stripMargin)
        sql(s"INSERT INTO $tblName VALUES (1, 'a@x.com')")
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val startVersion = deltaLog.update().version
        sql(s"ALTER TABLE $tblName ALTER COLUMN email DROP NOT NULL")
        sql(s"INSERT INTO $tblName VALUES (2, NULL)")
        sql(s"UPDATE $tblName SET email = 'b@x.com' WHERE email IS NULL")
        setNotNull(tblName, Seq("email"))
        val endVersion = deltaLog.update().version
        val changes = s"table_changes('$tblName', $startVersion, $endVersion)"

        val e = intercept[DeltaUnsupportedOperationException] {
          sql(s"SELECT * FROM $changes").collect()
        }
        assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")

        // This empty result is wrong; validation should reject the range as above.
        // Endpoint comparison misses the intermediate nullable schema. Detecting it requires
        // inspecting intermediate versions before Catalyst can prune the scan based on its
        // nullability rules and the end version's schema.
        // Fixing this requires a larger refactor of CDF validation logic into the analysis phase.
        checkAnswer(sql(s"SELECT id FROM $changes WHERE email IS NULL"), Seq.empty[Row])
      }
    }
  }

  for (renameColumns <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test("CDF across nested SET NOT NULL schema changes fails " +
        s"(renameColumns=$renameColumns)") {
      withSQLConf(
          DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "true") {
        val tblName = "contacts"
        val emailColumn = if (renameColumns) "renamed_email" else "email"
        val contactColumn = if (renameColumns) "renamed_contact" else "contact"
        withTable(tblName) {
          sql(s"""CREATE TABLE $tblName
                 |(id INT NOT NULL, email STRING,
                 | contact STRUCT<email: STRING> NOT NULL) USING $tableProvider
                 |TBLPROPERTIES (
                 |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '${NameMapping.name}'
                 |)""".stripMargin)
          sql(s"INSERT INTO $tblName VALUES (1, NULL, named_struct('email', NULL))")
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
          val startVersion = deltaLog.update().version
          sql(s"UPDATE $tblName SET email = 'a@x.com', contact.email = 'a@x.com'")
          val beforeConstraintVersion = deltaLog.update().version
          if (renameColumns) {
            sql(s"ALTER TABLE $tblName RENAME COLUMN email TO $emailColumn")
            sql(s"ALTER TABLE $tblName RENAME COLUMN contact TO $contactColumn")
          }
          setNotNull(tblName, Seq(emailColumn))
          setNotNull(tblName, Seq(contactColumn, "email"))
          val endVersion = deltaLog.update().version

          val changes = s"table_changes('$tblName', $startVersion, $endVersion)"
          val cdf = sql(s"SELECT * FROM $changes")
          Seq(emailColumn, s"$contactColumn.email").foreach { column =>
            val e = intercept[DeltaUnsupportedOperationException] {
              cdf.filter(col(column).isNull).collect()
            }
            assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")
          }

          // End-version mode must use the requested end schema, even after later constraints.
          val changes2 = s"table_changes('$tblName', $startVersion, $beforeConstraintVersion)"
          checkAnswer(sql(s"SELECT id FROM $changes2 WHERE contact.email IS NULL"),
            Seq(Row(1), Row(1)))
        }
      }
    }
  }

  // This test verifies that nullability overwrites during analysis do not prevent file pruning for
  // valid CDF scans.
  for (updates <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test("valid CDF null filters prune data file scans " +
        s"(updates=$updates)") {
      withSQLConf(
          DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> "true") {
        val tblName = "customers"
        withTable(tblName) {
          sql(s"""CREATE TABLE $tblName (id INT NOT NULL, email STRING NOT NULL)
                 |USING $tableProvider
                 |TBLPROPERTIES (
                 |  '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false'
                 |)""".stripMargin)
          sql(s"INSERT INTO $tblName VALUES (1, 'a@x.com')")
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
          val startVersion = deltaLog.update().version
          if (updates) {
            sql(s"UPDATE $tblName SET email = 'b@x.com'")
          } else {
            sql(s"INSERT INTO $tblName VALUES (2, 'b@x.com')")
          }
          val endVersion = deltaLog.update().version

          /** Checks if a query has a data file scan. */
          def hasDataFileScan(query: String, expected: Seq[Row]): Boolean = {
            val plans = DeltaTestUtils.withAllPlansCaptured(spark) {
              checkAnswer(sql(query), expected)
            }
            plans.exists(_.sparkPlan.exists(_.isInstanceOf[FileSourceScanExec]))
          }

          val baseQuery =
            s"SELECT id, _change_type FROM table_changes('$tblName', $startVersion, $endVersion)"
          val expected = if (updates) {
            Seq(Row(1, "insert"), Row(1, "update_preimage"), Row(1, "update_postimage"))
          } else {
            Seq(Row(1, "insert"), Row(2, "insert"))
          }
          assert(hasDataFileScan(baseQuery, expected))
          assert(!hasDataFileScan(s"$baseQuery WHERE id IS NULL", Seq.empty[Row]))
        }
      }
    }
  }
}

