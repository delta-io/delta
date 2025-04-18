/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.hook;

import io.delta.kernel.engine.Engine;
import java.io.IOException;

/**
 * A hook for executing operation after a transaction commit. Hooks are added in the Transaction and
 * engine need to invoke the hook explicitly for executing the operation. Supported operations are
 * listed in {@link PostCommitHookType}.
 */
public interface PostCommitHook {

  enum PostCommitHookType {
    /**
     * Writes a new checkpoint at the version committed by the transaction. This hook is present
     * when the table is ready for checkpoint according to its configured checkpoint interval. To
     * perform this operation, reading previous checkpoint + logs is required to construct a new
     * checkpoint, with latency scaling based on log size (typically seconds to minutes).
     */
    CHECKPOINT,
    /**
     * Writes a checksum file at the version committed by the transaction. This hook is present when
     * all required table statistics (e.g. table size) for checksum file are known when a
     * transaction commits. This operation has a minimal latency with no requirement of reading
     * previous checkpoint or logs.
     */
    CHECKSUM_SIMPLE,

    CHECKSUM_FULL,

    /**
     * Writes a log compaction file that merges a range of commit JSON files into a single file.
     * This hook is triggered on a configurable interval (e.g., every 10 commits) and reduces the
     * number of small log files that need to be read when reconstructing the table state, thereby
     * improving read performance.
     */
    LOG_COMPACTION
  }

  /** Invokes the post commit operation whose implementation must be thread safe. */
  void threadSafeInvoke(Engine engine) throws IOException;

  PostCommitHookType getType();
}
