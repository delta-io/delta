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

import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.redirect.{
  DropRedirectInProgress,
  EnableRedirectInProgress,
  PathBasedRedirectSpec,
  RedirectReaderWriter,
  RedirectReady,
  RedirectState
}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class TableRedirectSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite
  with DeltaCheckpointTestUtils
  with DeltaSQLTestUtils {

  private def validateState(
      deltaLog: DeltaLog,
      redirectState: RedirectState,
      destTablePath: File
  ): Unit = {
    val snapshot = deltaLog.update()
    assert(RedirectReaderWriter.isFeatureSet(snapshot.metadata))
    val redirectConfig = RedirectReaderWriter.getRedirectConfiguration(snapshot.metadata).get
    assert(snapshot.protocol.supportsReaderFeatures && snapshot.protocol.supportsWriterFeatures)
    assert(snapshot.protocol.readerFeatureNames.contains(RedirectReaderWriterFeature.name))
    assert(snapshot.protocol.writerFeatureNames.contains(RedirectReaderWriterFeature.name))
    assert(redirectConfig.redirectState == redirectState)
    assert(redirectConfig.`type` == PathBasedRedirectSpec.REDIRECT_TYPE)
    val expectedSpecValue = s"""{"tablePath":"${destTablePath.getCanonicalPath}"}"""
    assert(redirectConfig.specValue == expectedSpecValue)
    val redirectSpec = redirectConfig.spec.asInstanceOf[PathBasedRedirectSpec]
    assert(redirectSpec.tablePath == destTablePath.getCanonicalPath)
  }

  test("basic table redirect") {
    withTempDir { sourceTablePath =>
      withTempDir { destTablePath =>
        sql(s"CREATE external TABLE t1(c0 long, c1 long) USING delta LOCATION '$sourceTablePath';")
        val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        val deltaLog = DeltaLog.forTable(spark, new Path(sourceTablePath.getCanonicalPath))
        var snapshot = deltaLog.update()
        assert(!RedirectReaderWriter.isFeatureSet(snapshot.metadata))
        val redirectSpec = new PathBasedRedirectSpec(destTablePath.getCanonicalPath)
        // Step-1: Initiate table redirection and set to EnableRedirectInProgress state.
        RedirectReaderWriter.add(
          deltaLog,
          Some(catalogTable),
          PathBasedRedirectSpec.REDIRECT_TYPE,
          redirectSpec
        )
        validateState(deltaLog, EnableRedirectInProgress, destTablePath)
        // Step-2: Complete table redirection and set to RedirectReady state.
        RedirectReaderWriter.update(
          deltaLog,
          Some(catalogTable),
          RedirectReady,
          redirectSpec
        )
        validateState(deltaLog, RedirectReady, destTablePath)
        // Step-3: Start dropping table redirection and set to DropRedirectInProgress state.
        RedirectReaderWriter.update(
          deltaLog,
          Some(catalogTable),
          DropRedirectInProgress,
          redirectSpec
        )
        validateState(deltaLog, DropRedirectInProgress, destTablePath)
        // Step-4: Finish dropping table redirection and remove the property completely.
        RedirectReaderWriter.remove(deltaLog, Some(catalogTable))
        snapshot = deltaLog.update()
        assert(!RedirectReaderWriter.isFeatureSet(snapshot.metadata))
        assert(snapshot.protocol.supportsReaderFeatures && snapshot.protocol.supportsWriterFeatures)
        assert(snapshot.protocol.readerFeatureNames.contains(RedirectReaderWriterFeature.name))
        assert(snapshot.protocol.writerFeatureNames.contains(RedirectReaderWriterFeature.name))
      }
    }
  }
}
