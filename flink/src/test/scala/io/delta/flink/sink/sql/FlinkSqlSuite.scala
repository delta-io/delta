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

  test("load hadoop table") {
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

  test("load partitioned hadoop table") {
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
