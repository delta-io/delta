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

package org.apache.spark.sql.delta.rowid

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.RowIdFeature
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{defaultPropertyKey, propertyKey}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

trait RowIdTestUtils extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  val rowIdFeatureName: String = propertyKey(RowIdFeature)
  val defaultRowIdFeatureProperty: String = defaultPropertyKey(RowIdFeature)

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.ROW_IDS_ALLOWED.key, "true")

  def withRowIdsEnabled(enabled: Boolean)(f: => Unit): Unit = {
    // Even when we don't want Row Ids on created tables, we want to enable code paths that
    // interact with them, which is controlled by this config.
    assert(spark.conf.get(DeltaSQLConf.ROW_IDS_ALLOWED.key) == "true")
    val configPairs = if (enabled) Seq(defaultRowIdFeatureProperty -> "enabled") else Seq.empty
    withSQLConf(configPairs: _*)(f)
  }
}
