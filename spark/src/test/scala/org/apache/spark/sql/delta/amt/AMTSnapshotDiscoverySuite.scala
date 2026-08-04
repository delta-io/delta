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

import org.apache.spark.sql.delta.util.FileNames

class AMTSnapshotDiscoverySuite extends AMTCheckpointTestBase {

  ////////////////////////////
  // Cold snapshot discovery
  ////////////////////////////

  ///////////////////////////
  // deltaLog.update()
  ///////////////////////////

  ///////////////////////////
  // Post commit snapshot
  ///////////////////////////

  testAcrossAMTCheckpointScenarios(
      "emission installs an AMTCheckpointProvider on the post-commit snapshot",
      "amt_provider_install")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    // The harness checks the provider on the snapshot it re-read from the log. This test covers the
    // post-commit path specifically: the emission must install the provider on the in-memory
    // `unsafeVolatileSnapshot` the commit produced, without waiting for a fresh log read.
    val postCommit = context.postCheckpointSnapshot.deltaLog.unsafeVolatileSnapshot
    assert(postCommit.version == context.manifestCommitVersion,
      s"The post-commit snapshot must be at v${context.manifestCommitVersion}; " +
        s"got v${postCommit.version}.")
    val provider = amtProvider(postCommit).getOrElse(
      fail("The post-commit snapshot must expose an AMTCheckpointProvider."))
    assert(provider.checkpointVersion == context.checkpoint.version,
      s"The provider must describe v${context.checkpoint.version}; " +
        s"got v${provider.checkpointVersion}.")
    assert(provider.checkpointAction.contentRoot.path == context.checkpoint.contentRoot.path,
      "The provider must point at the emitted checkpoint's root manifest.")
  }

  testAcrossAMTCheckpointScenarios(
      "an emitted AMT installs the provider and trims the log segment",
      "amt_log_segment")(
      setup = name => (1 to 2).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (3)"))) { context =>
    val segmentDeltaVersions =
      context.postCheckpointSnapshot.logSegment.deltas.map(f => FileNames.deltaVersion(f))
    assert(segmentDeltaVersions.forall(_ > context.checkpoint.version),
      s"Log segment must trim deltas up to the checkpoint version; got $segmentDeltaVersions.")
  }
}
