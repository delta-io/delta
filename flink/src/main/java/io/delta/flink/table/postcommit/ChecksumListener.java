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

import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.TableEventListener;
import io.delta.kernel.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Post-commit listener that generate checksum for the snapshot */
public class ChecksumListener implements TableEventListener {

  public static final Logger LOG = LoggerFactory.getLogger(ChecksumListener.class);

  @Override
  public void onPostCommit(AbstractKernelTable source, Snapshot snapshot) {
    if (source.getConf().isChecksumEnabled()) {
      // Write checksum asynchronously
      source.executeWithTiming(
          "postcommit.checksum",
          () -> snapshot.writeChecksum(source.getEngine(), Snapshot.ChecksumWriteMode.SIMPLE));
    }
  }
}
