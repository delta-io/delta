/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.sql

import java.nio.file.Files

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession

class DeltaSparkSessionExtensionSuite extends SparkFunSuite {

  private def verifyDeltaSQLParserIsActivated(spark: SparkSession): Unit = {
    val input = Files.createTempDirectory("DeltaSparkSessionExtensionSuite").toFile
    try {
      spark.range(1, 10).write.format("delta").save(input.getCanonicalPath)
      spark.sql(s"vacuum delta.`${input.getCanonicalPath}`")
    } finally {
      JavaUtils.deleteRecursively(input)
    }
  }

  test("activate Delta SQL parser using SQL conf") {
    val spark = SparkSession.builder()
      .appName("DeltaSparkSessionExtensionSuiteUsingSQLConf")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    try {
      verifyDeltaSQLParserIsActivated(spark)
    } finally {
      spark.close()
    }
  }

  test("activate Delta SQL parser using withExtensions") {
    val spark = SparkSession.builder()
      .appName("DeltaSparkSessionExtensionSuiteUsingWithExtensions")
      .master("local[2]")
      .withExtensions(new io.delta.sql.DeltaSparkSessionExtension)
      .getOrCreate()
    try {
      verifyDeltaSQLParserIsActivated(spark)
    } finally {
      spark.close()
    }
  }
}
