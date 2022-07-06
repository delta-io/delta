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
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

import java.io.File
import scala.reflect.io.Directory

class DeltaShowCreateTableSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test(testName = "Test DDL Output for External Table SHOW CREATE TABLE") {
    withTempDir {  foo =>
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

  test(testName = "Test DDL Output for Managed Table SHOW CREATE TABLE") {
    val table = "managed_table"
    sql(s"CREATE TABLE `$table` (id LONG) USING delta")
    val ddl = getShowCreateTable(table)
    assert(ddl.contains("CREATE TABLE"))
    assert(!ddl.contains("'external' = 'false'"))
    assert(ddl.contains("'Type' = 'MANAGED'"))
    assert(ddl.contains("USING delta"))
    deleteTableAndData(table)
  }

  test(testName = "Test Recreate table using DDL SHOW CREATE TABLE") {
    val table = "managed_table"
    sql(s"CREATE TABLE `$table` (id LONG) USING delta")
    val ddl = getShowCreateTable(table)
    deleteTableAndData(table)
    sql(ddl)
    deleteTableAndData(table)
  }

  test(testName = "Test DDL Idempotency SHOW CREATE TABLE") {
    val table = "managed_table"
    sql(s"CREATE TABLE `$table` (id LONG) USING delta")
    val ddl = getShowCreateTable(table)
    deleteTableAndData(table)
    sql(ddl)
    val ddl1 = getShowCreateTable(table)
    deleteTableAndData(table)
    assert(ddl1.equals(ddl)) // table DDL are the same
  }

  test(testName = "Test DDL Comment SHOW CREATE TABLE") {
    val table = "managed_table"
    val comment = "This is a random comment on the table"
    sql(s"CREATE TABLE `$table` (id LONG) USING delta COMMENT '$comment'")
    val ddl = getShowCreateTable(table)
    deleteTableAndData(table)
    assert(ddl.contains(s"COMMENT '$comment'"))
  }

  test(testName = "Test DDL Partition SHOW CREATE TABLE") {
    val table = "managed_table"
    sql(
      s"""CREATE TABLE $table (id INT, name STRING, age INT) USING delta
         | PARTITIONED BY (age)""".stripMargin)
    val ddl = getShowCreateTable(table)
    assert(ddl.contains("PARTITIONED BY (age)"))
    deleteTableAndData(table)
  }

  test(testName = "Test DDL Random Table Property SHOW CREATE TABLE") {
    val table = "managed_table"
    sql(
      s"""CREATE TABLE $table (id INT, name STRING, age INT) USING delta
         | PARTITIONED BY (age) TBLPROPERTIES
         | ('foo'='bar','bar'='foo')""".stripMargin)
    val ddl = getShowCreateTable(table)
    deleteTableAndData(table)
    assert(ddl.contains("PARTITIONED BY (age)"))
    assert(ddl.contains("TBLPROPERTIES"))
    assert(ddl.contains("'foo' = 'bar'"))
    assert(ddl.contains("'bar' = 'foo'"))
  }

  test(testName = "Test DDL with full variations SHOW CREATE TABLE") {
    withTempDir { foo =>
      val table = "some_external_table"
      val fooPath = foo.getCanonicalPath
      sql(
        s"""CREATE TABLE $table (id INT COMMENT "some id", name STRING, age INT) USING delta
           | LOCATION "$fooPath"
           | PARTITIONED BY (age)
           | TBLPROPERTIES
           | ('foo'='bar','bar'='foo')
           | COMMENT "some comment"""".stripMargin)
      val ddl = getShowCreateTable(table)
      deleteTableAndData(table)
      assert(ddl.contains("CREATE TABLE"))
      assert(ddl.contains("`id` INT COMMENT 'some id'"))
      assert(ddl.contains("'Type' = 'EXTERNAL'"))
      assert(ddl.contains(s"LOCATION 'file:$fooPath'"))
      assert(ddl.contains("USING delta"))
      assert(ddl.contains("PARTITIONED BY (age)"))
      assert(ddl.contains("TBLPROPERTIES"))
      assert(ddl.contains("'foo' = 'bar'"))
      assert(ddl.contains("'bar' = 'foo'"))
    }
  }

  test(testName = "Test DDL with full variations Recreate from DDL SHOW CREATE TABLE") {
    withTempDir { foo =>
      val table = "some_external_table"
      val fooPath = foo.getCanonicalPath
      sql(
        s"""CREATE TABLE $table (id INT COMMENT "some id", name STRING, age INT) USING delta
           | LOCATION "$fooPath"
           | PARTITIONED BY (age)
           | TBLPROPERTIES
           | ('foo'='bar','bar'='foo')
           | COMMENT "some comment"""".stripMargin)
      val ddl = getShowCreateTable(table)
      deleteTableAndData(table)
      sql(ddl)
      val ddl1 = getShowCreateTable(table)
      assert(ddl.equals(ddl1))
    }
  }

  /**
   * Delete managed table and ensure that the disk cleanup occurs
   * right away so the table can be immediately recreated
   * @param tableName name of the table
   */
  def deleteTableAndData(tableName: String): Unit = {
    val ddf = sql(s"DESCRIBE FORMATTED $tableName")
    val location = ddf.collect.toSeq.filter(x =>
      x.get(0).toString=="Location").head.get(1).toString
    sql(s"DROP TABLE `$tableName`")
    val directory = new Directory(new File(location.substring(5)))
    directory.deleteRecursively()
  }

  /**
   * Helper method to get the show create table output as a string
   * @param tableName name of the table
   * @return ddl of the table
   */
  def getShowCreateTable(tableName: String): String = {
    val df = sql(s"SHOW CREATE TABLE $tableName")
    val rows = df.collectAsList()
    rows.get(0).get(0).toString
  }
}