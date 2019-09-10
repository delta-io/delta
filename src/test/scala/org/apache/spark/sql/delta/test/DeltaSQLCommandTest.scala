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

package org.apache.spark.sql.delta.test

import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.test.{TestSparkSession, SharedSparkSession}

/**
 * Because `TestSparkSession` doesn't pick up the conf `spark.sql.extensions` in Spark 2.4.x, we use
 * this class to inject Delta's extension in our tests.
 *
 * @see https://issues.apache.org/jira/browse/SPARK-25003
 */
class DeltaTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    extensions
  }
}

/**
 * A trait for tests that are testing Delta's own SQL commands. This will set up Delta's extension
 * for tests running with Spark 2.4.x.
 */
trait DeltaSQLCommandTest { self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new DeltaTestSparkSession(sparkConf)
  }
}
