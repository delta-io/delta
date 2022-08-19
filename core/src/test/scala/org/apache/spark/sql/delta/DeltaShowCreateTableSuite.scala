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

// scalastyle:off import.ordering.noEmptyLine

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

import java.io.File
import scala.reflect.io.Directory

class DeltaShowCreateTableSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("Test DDL Output for External Table SHOW CREATE TABLE") {
    withTempDir { foo =>
      withTable("external_table") {
        val table = "external_table"
        val fooPath = foo.getCanonicalPath
        sql(s"CREATE TABLE `$table` (id LONG) USING delta LOCATION '$fooPath'")
        val ddl = getShowCreateTable(table)
        assert(ddl.contains("CREATE TABLE"))
        assert(ddl.contains("'external' = 'true'"))
        assert(ddl.contains(s"LOCATION 'file:$fooPath'"))
        assert(ddl.contains("USING delta"))
      }
    }
  }

  test("Test DDL Output for External Table SHOW CREATE TABLE using String Equals") {
    withTempDir { foo =>
      withTable("external_table") {
        val table = "external_table"
        val fooPath = foo.getCanonicalPath
        sql(s"CREATE TABLE `$table` (id LONG) USING delta LOCATION '$fooPath'")
        val ddl = getShowCreateTable(table)
        assert(ddl == s"""CREATE TABLE default.$table (
                         |  `id` BIGINT)
                         |USING delta
                         |LOCATION 'file:$fooPath'
                         |TBLPROPERTIES (
                         |  'Type' = 'EXTERNAL',
                         |  'delta.minReaderVersion' = '1',
                         |  'delta.minWriterVersion' = '2',
                         |  'external' = 'true')
                         |""".stripMargin)
      }
    }
  }

  test("Test DDL Output for Managed Table SHOW CREATE TABLE") {
    val table = "managed_table"
    withTable(table) {
      sql(s"CREATE TABLE `$table` (id LONG) USING delta")
      val ddl = getShowCreateTable(table)
      assert(ddl.contains("CREATE TABLE"))
      assert(!ddl.contains("'external' = 'false'"))
      assert(ddl.contains("'Type' = 'MANAGED'"))
      assert(ddl.contains("USING delta"))
    }
  }

  test("Test Recreate table using DDL SHOW CREATE TABLE") {
    val table = "managed_table"
    var ddl = ""
    withTable(table) {
      sql(s"CREATE TABLE `$table` (id LONG) USING delta")
      ddl = getShowCreateTable(table)
    }

    withTable(table) {
      sql(ddl)
    }
  }

  test("Test DDL Idempotency SHOW CREATE TABLE") {
    val table = "managed_table"
    var ddl, ddl1 = ""
    withTable(table) {
      sql(s"CREATE TABLE `$table` (id LONG) USING delta")
      ddl = getShowCreateTable(table)
    }

    withTable(table) {
      sql(ddl)
      ddl1 = getShowCreateTable(table)
    }
    assert(ddl1.equals(ddl)) // table DDL are the same
  }

  test("Test DDL Comment SHOW CREATE TABLE") {
    val table = "managed_table"
    val comment = "This is a random comment on the table"
    withTable(table) {
      sql(s"CREATE TABLE `$table` (id LONG) USING delta COMMENT '$comment'")
      val ddl = getShowCreateTable(table)
      assert(ddl.contains(s"COMMENT '$comment'"))
    }
  }

  test("Test DDL Partition SHOW CREATE TABLE") {
    val table = "managed_table"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table (id INT, name STRING, age INT) USING delta
           | PARTITIONED BY (age)""".stripMargin)
      val ddl = getShowCreateTable(table)
      assert(ddl.contains("PARTITIONED BY (age)"))
    }
  }

  test("Test DDL Random Table Property SHOW CREATE TABLE") {
    val table = "managed_table"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table (id INT, name STRING, age INT) USING delta
           | PARTITIONED BY (age) TBLPROPERTIES
           | ('foo'='bar','bar'='foo')""".stripMargin)
      val ddl = getShowCreateTable(table)
      assert(ddl.contains("PARTITIONED BY (age)"))
      assert(ddl.contains("TBLPROPERTIES"))
      assert(ddl.contains("'foo' = 'bar'"))
      assert(ddl.contains("'bar' = 'foo'"))
    }
  }

  test("Test DDL Random Option SHOW CREATE TABLE") {
    val table = "managed_table"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table (id INT, name STRING, age INT) USING delta
           | OPTIONS ('foo'='bar','bar'='foo')""".stripMargin)
      val ddl = getShowCreateTable(table)
      assert(ddl.contains("OPTIONS"))
      assert(ddl.contains("'foo' = 'bar'"))
      assert(ddl.contains("'bar' = 'foo'"))
    }
  }

  test("Test DDL with full variations SHOW CREATE TABLE") {
    withTempDir { foo =>
      val table = "some_external_table"
      withTable(table) {
        val fooPath = foo.getCanonicalPath
        sql(
          s"""CREATE TABLE $table (id INT COMMENT "some id", name STRING, age INT) USING delta
             | LOCATION "$fooPath"
             | OPTIONS ('option1'='test')
             | PARTITIONED BY (age)
             | TBLPROPERTIES
             | ('foo'='bar','bar'='foo')
             | COMMENT "some comment"""".stripMargin)
        val ddl = getShowCreateTable(table)
        assert(ddl.contains("CREATE TABLE"))
        assert(ddl.contains("`id` INT COMMENT 'some id'"))
        assert(ddl.contains("'Type' = 'EXTERNAL'"))
        assert(ddl.contains(s"LOCATION 'file:$fooPath'"))
        assert(ddl.contains("USING delta"))
        assert(ddl.contains("PARTITIONED BY (age)"))
        assert(ddl.contains("TBLPROPERTIES"))
        assert(ddl.contains("'foo' = 'bar'"))
        assert(ddl.contains("'bar' = 'foo'"))
        assert(ddl.contains("OPTIONS"))
        assert(ddl.contains("'option1' = 'test'"))
      }
    }
  }

  test("Test Generated Column Results in Exception for SHOW CREATE TABLE") {
    val table = "people10m"
    withTable(table) {
      io.delta.tables.DeltaTable.create(spark)
        .tableName(table)
        .addColumn("id", "INT")
        .addColumn("birthDate", "TIMESTAMP")
        .addColumn(io.delta.tables.DeltaTable.columnBuilder("c2")
          .dataType(DateType)
          .generatedAlwaysAs("CAST(birthDate AS DATE)")
          .build())
        .execute()
      val e = intercept[UnsupportedOperationException] {
        getShowCreateTable(table)
      }
      assert(e.getMessage.contains("contains generated columns"))
    }
  }

  test("Test DDL with full variations Recreate from DDL SHOW CREATE TABLE") {
    val table = "some_external_table"
    var ddl = ""
    withTempDir { foo =>
      val fooPath = foo.getCanonicalPath
      withTable(table) {
        sql(
          s"""CREATE TABLE $table (id INT COMMENT "some id", name STRING, age INT) USING delta
             | LOCATION "$fooPath"
             | PARTITIONED BY (age)
             | TBLPROPERTIES
             | ('foo'='bar','bar'='foo')
             | COMMENT "some comment"""".stripMargin)
        ddl = getShowCreateTable(table)
      }

      // delete the underlying external table
      val directory = new Directory(new File(fooPath))
      directory.deleteRecursively()

      withTable(table) {
        sql(ddl)
        val ddl1 = getShowCreateTable(table)
        assert(ddl.equals(ddl1))
      }
    }
  }

  test("Test DDL Output for Table with File Path SHOW CREATE TABLE") {
    withTempDir { foo =>
      withTable("external_table") {
        val fooPath = foo.getCanonicalPath
        sql(s"CREATE TABLE delta.`file:$fooPath` (id bigint) USING delta")
        val ddl = getShowCreateTable(s"delta.`$fooPath`")
        assert(ddl.contains("CREATE TABLE"))
        assert(ddl.contains("USING delta"))
        assert(ddl.contains(s"TABLE delta.`file:$fooPath`"))
        assert(ddl.contains(s"LOCATION '$fooPath'"))
      }
    }
  }
  /**
   * Helper method to get the show create table output as a string
   *
   * @param tableName name of the table
   * @return ddl of the table
   */
  def getShowCreateTable(tableName: String): String = {
    lazy val sparkSession: SparkSession = SparkSession.active
    import sparkSession.implicits._
    sql(s"SHOW CREATE TABLE $tableName").as[String].collect().head
  }
}
