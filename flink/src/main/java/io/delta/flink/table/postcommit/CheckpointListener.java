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

package io.delta.flink.table.postcommit;

import io.delta.flink.kernel.Checkpoint;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.TableEventListener;
import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;

public class CheckpointListener implements TableEventListener {

  @Override
  public void onPostCommit(AbstractKernelTable source, Snapshot snapshot) {
    if (source.getConf().shouldCreateCheckpoint()) {
      if (snapshot instanceof SnapshotImpl
          && ((SnapshotImpl) snapshot)
              .getProtocol()
              .getWriterFeatures()
              .contains(TableFeatures.CHECKPOINT_V2_RW_FEATURE.featureName())) {
        // Use v2 incremental checkpoint when possible
        source.executeWithTiming(
            "commit.checkpoint", () -> new Checkpoint(source.getEngine(), snapshot).write());
      } else {
        source.executeWithTiming(
            "commit.checkpoint", () -> snapshot.writeCheckpoint(source.getEngine()));
      }
    }
  }
}
