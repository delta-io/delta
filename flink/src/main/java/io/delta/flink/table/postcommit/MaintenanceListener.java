/*
 *  Copyright (2026) The Delta Lake Project Authors.
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

import dev.failsafe.function.CheckedRunnable;
import io.delta.flink.kernel.CheckpointWriter;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.TableEventListener;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This post-commit listener will publish the snapshot, and optionally create checkpoints. */
public class MaintenanceListener implements TableEventListener {

  @Override
  public void onPostCommit(AbstractKernelTable source, Snapshot snapshot) {
    source.executeWithTiming("postcommit.maintenance", new MaintenanceTask(source, snapshot));
  }

  static class MaintenanceTask implements CheckedRunnable {

    static final Logger LOG = LoggerFactory.getLogger(MaintenanceTask.class);
    final AbstractKernelTable table;
    final Snapshot snapshot;

    public MaintenanceTask(AbstractKernelTable table, Snapshot snapshot) {
      this.table = table;
      this.snapshot = snapshot;
    }

    public void run() {
      Engine engine = table.getEngine();
      try {
        // Publish commits
        Snapshot published =
            table.withTiming("postcommit.maintenance.publish", () -> snapshot.publish(engine));
        // Update cache
        table.getCacheManager().put(table.getTablePath().toString(), published);
        // Checkpoint can be done only on published snapshots
        if (table.getConf().shouldCreateCheckpoint()) {
          if (snapshot instanceof SnapshotImpl
              && ((SnapshotImpl) snapshot)
                  .getProtocol()
                  .getWriterFeatures()
                  .contains(TableFeatures.CHECKPOINT_V2_RW_FEATURE.featureName())) {
            // Use v2 incremental checkpoint when possible
            table.withTiming(
                "postcommit.maintenance.checkpoint",
                () -> new CheckpointWriter(engine, snapshot).write());
          } else {
            table.withTiming(
                "postcommit.maintenance.checkpoint", () -> snapshot.writeCheckpoint(engine));
          }
        }
      } catch (Exception e) {
        LOG.error("Exception while maintenance", e);
      }
    }
  }
}
