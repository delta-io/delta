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

package org.apache.spark.sql.delta.amt

import java.util.Locale

import org.apache.spark.sql.delta.{
  AdaptiveMetadataTableFeature,
  ColumnMappingTableFeature,
  DeltaLog,
  DeltaTableFeatureException,
  FeatureAutomaticallyEnabledByMetadata,
  RemovableFeature,
  TableFeature
}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class AdaptiveMetadataTableFeatureSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("feature is a ReaderWriterFeature and is NOT a RemovableFeature") {
    assert(AdaptiveMetadataTableFeature.isReaderWriterFeature)
    // TODO(v4amt)(LC-14961): Fix this once downgrade support is added.
    assert(!AdaptiveMetadataTableFeature.isInstanceOf[RemovableFeature])
  }

  test("feature is not automatically enabled by metadata") {
    assert(!AdaptiveMetadataTableFeature.isInstanceOf[FeatureAutomaticallyEnabledByMetadata])
  }

  test("feature requires column mapping") {
    assert(AdaptiveMetadataTableFeature.requiredFeatures === Set(ColumnMappingTableFeature))
  }

  test("creating a Delta table with delta.feature.adaptiveMetadata-preview=supported succeeds") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""CREATE TABLE delta.`$path` (id INT) USING DELTA
           |TBLPROPERTIES ('${propertyKey(AdaptiveMetadataTableFeature)}' =
           |  '$FEATURE_PROP_SUPPORTED')""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, path)
      val protocol = deltaLog.update().protocol
      assert(protocol.isFeatureSupported(AdaptiveMetadataTableFeature),
        s"Protocol must record adaptiveMetadata-preview as supported. Got: $protocol")
    }
  }

  test("enabling the feature is rejected when the kill-switch SQL conf is false") {
    withSQLConf(DeltaSQLConf.V4_ADAPTIVE_METADATA_TABLE_PREVIEW_ENABLED.key -> "false") {
      // The feature is unregistered, so featureNameToFeature returns None.
      assert(TableFeature.featureNameToFeature(AdaptiveMetadataTableFeature.name).isEmpty)

      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val ex = intercept[DeltaTableFeatureException] {
          sql(
            s"""CREATE TABLE delta.`$path` (id INT) USING DELTA
               |TBLPROPERTIES ('${propertyKey(AdaptiveMetadataTableFeature)}' =
               |  '$FEATURE_PROP_SUPPORTED')""".stripMargin)
        }
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("unsupported"),
          s"Expected an unsupported-feature error, got: ${ex.getMessage}")
      }
    }
  }
}
