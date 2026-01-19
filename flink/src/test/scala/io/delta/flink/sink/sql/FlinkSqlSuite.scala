/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.sql

import io.delta.flink.TestHelper
import io.delta.kernel.types.{LongType, StringType, StructType}

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.scalatest.funsuite.AnyFunSuite

class FlinkSqlSuite extends AnyFunSuite with TestHelper {

  test("load into hadoop table") {
    withTempDir { dir =>
      val settings = EnvironmentSettings.newInstance.inStreamingMode.build
      val tEnv = TableEnvironment.create(settings)
      val numRecords = 5000
      tEnv.executeSql(
        s"""
           CREATE TEMPORARY TABLE src (
           id BIGINT,
           dt STRING
           ) WITH (
           'connector' = 'datagen',
           'rows-per-second' = '1000',
           'fields.id.kind' = 'sequence',
           'fields.id.start' = '1',
           'fields.id.end' = '$numRecords'
           )""".stripMargin)

      tEnv.executeSql(
        s"""
          CREATE TEMPORARY TABLE sink (
            id BIGINT,
            dt STRING
          ) WITH (
            'connector' = 'delta',
            'table_path' = '${dir.getPath}',
            'sink.parallelism' = '4',
            'uid' = 'someuid'
          )
        """.stripMargin)

      tEnv.executeSql("""INSERT INTO sink SELECT id, dt FROM src""".stripMargin).await()

      val schema = new StructType()
        .add("id", LongType.LONG)
        .add("dt", StringType.STRING)
      // Check the table content
      verifyTableContent(
        dir.getPath,
        (_, addfiles, properties) => {
          assert(numRecords == addfiles.map(_.getNumRecords.get().longValue()).sum)
          val records = addfiles.flatMap { addfile =>
            readParquet(dir.toPath.resolve(addfile.getPath), schema).map(_.getLong(0))
          }.toSet
          assert((1 to numRecords).toSet == records)
        })
    }
  }

  test("load into partitioned hadoop table") {
    withTempDir { dir =>
      val settings = EnvironmentSettings.newInstance.inStreamingMode.build
      val tEnv = TableEnvironment.create(settings)
      val numRecords = 1000
      tEnv.executeSql(
        s"""
           CREATE TEMPORARY TABLE src (
           id BIGINT,
           dt STRING
           ) WITH (
           'connector' = 'datagen',
           'number-of-rows' = '$numRecords',
           'rows-per-second' = '100',
           'fields.id.kind' = 'sequence',
           'fields.id.start' = '1',
           'fields.id.end' = '$numRecords',
           'fields.dt.kind' = 'random',
           'fields.dt.length' = '1'
           )""".stripMargin)

      tEnv.executeSql(
        s"""
          CREATE TEMPORARY TABLE sink (
            id BIGINT,
            dt STRING
          ) WITH (
            'connector' = 'delta',
            'table_path' = '${dir.getPath}',
            'partitions' = 'dt',
            'uid' = 'someuid'
          )
        """.stripMargin)

      tEnv.executeSql("""INSERT INTO sink SELECT id, dt FROM src""".stripMargin).await()

      val schema = new StructType()
        .add("id", LongType.LONG)
        .add("dt", StringType.STRING)
      // Check the table content
      verifyTableContent(
        dir.getPath,
        (_, addfiles, properties) => {
          assert(numRecords == addfiles.map(_.getNumRecords.get().longValue()).sum)
          assert((1 to numRecords).toSet == addfiles.flatMap { addfile =>
            readParquet(dir.toPath.resolve(addfile.getPath), schema).map(_.getLong(0))
          }.toSet)
          assert(16 == addfiles.map { a => a.getPartitionValues.getValues.getString(0) }.toSet.size)
        })
    }
  }

  test("load into table with many types") {
    withTempDir { dir =>
      val settings = EnvironmentSettings.newInstance.inStreamingMode.build
      val tEnv = TableEnvironment.create(settings)
      val numRecords = 1000
      tEnv.executeSql(
        s"""
           CREATE TABLE src (
              c_tinyint    TINYINT,
              c_smallint   SMALLINT,
              c_int        INT,
              c_bigint     BIGINT,
              c_decimal    DECIMAL(18, 6),
              c_float      FLOAT,
              c_double     DOUBLE,
              c_bool       BOOLEAN,
              c_char       CHAR(8),
              c_varchar    VARCHAR(32),
              c_string     STRING,
              c_binary     BINARY(8),
              c_varbinary  VARBINARY(16),
              c_bytes      BYTES,
              c_date       DATE,
              c_time       TIME(0)
           ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '$numRecords',
              'rows-per-second' = '500',
              'fields.c_tinyint.min' = '0',
              'fields.c_tinyint.max' = '100',
              'fields.c_smallint.min' = '0',
              'fields.c_smallint.max' = '1000',
              'fields.c_int.min' = '0',
              'fields.c_int.max' = '100000',
              'fields.c_bigint.min' = '0',
              'fields.c_bigint.max' = '1000000000',
              'fields.c_decimal.min' = '0',
              'fields.c_decimal.max' = '999999',
              'fields.c_float.min' = '0',
              'fields.c_float.max' = '100',
              'fields.c_double.min' = '0',
              'fields.c_double.max' = '1000',
              'fields.c_bool.kind' = 'random',
              'fields.c_varchar.length' = '16',
              'fields.c_string.length' = '24',
              'fields.c_varbinary.length' = '16',
              'fields.c_bytes.length' = '12'
           )""".stripMargin)

      tEnv.executeSql(
        s"""
          CREATE TEMPORARY TABLE sink (
            c_tinyint    TINYINT,
            c_smallint   SMALLINT,
            c_int        INT,
              c_bigint     BIGINT,
              c_decimal    DECIMAL(18, 6),
              c_float      FLOAT,
              c_double     DOUBLE,
              c_bool       BOOLEAN,
              c_char       CHAR(8),
              c_varchar    VARCHAR(32),
              c_string     STRING,
              c_binary     BINARY(8),
              c_varbinary  VARBINARY(16),
              c_bytes      BYTES,
              c_date       DATE,
              c_time       TIME(0)
          ) WITH (
            'connector' = 'delta',
            'table_path' = '${dir.getPath}',
            'uid' = 'someuid'
          )
        """.stripMargin)

      tEnv.executeSql(
        """INSERT INTO sink SELECT
          | c_tinyint,
          | c_smallint,
          | c_int,
          | c_bigint,
          | c_decimal,
          | c_float,
          | c_double,
          | c_bool,
          | c_char,
          | c_varchar,
          | c_string,
          | c_binary,
          | c_varbinary,
          | c_bytes,
          | c_date,
          | c_time
          | FROM src""".stripMargin).await()

    }
  }

  ignore("load uc table") {
    val settings = EnvironmentSettings.newInstance.inStreamingMode.build
    val tEnv = TableEnvironment.create(settings)
    val numRecords = 5000
    tEnv.executeSql(
      s"""
           CREATE TEMPORARY TABLE src (
           id INT,
           name STRING
           ) WITH (
           'connector' = 'datagen',
           'rows-per-second' = '1000',
           'fields.id.kind' = 'sequence',
           'fields.id.start' = '1',
           'fields.id.end' = '$numRecords'
           )""".stripMargin)

    tEnv.executeSql(
      s"""
         CREATE CATALOG main WITH (
            'type' = 'unitycatalog',
            'endpoint' = 'https://e2-dogfood.staging.cloud.databricks.com/',
            'token' = '<REMOVED>'
         )""".stripMargin)

    tEnv.executeSql("""INSERT INTO main.hao.writetest SELECT id, name FROM src"""
      .stripMargin).await()

  }
}
