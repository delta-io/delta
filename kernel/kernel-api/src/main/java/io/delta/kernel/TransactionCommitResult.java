/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.utils.CloseableIterable;
import java.util.List;

/**
 * Contains the result of a successful transaction commit. Returned by {@link
 * Transaction#commit(Engine, CloseableIterable)}.
 *
 * @since 3.2.0
 */
@Evolving
public class TransactionCommitResult {
  private final long version;
  private final List<PostCommitHook> postCommitHooks;

  public TransactionCommitResult(long version, List<PostCommitHook> postCommitHooks) {
    this.version = version;
    this.postCommitHooks = postCommitHooks;
  }

  /**
   * Contains the version of the transaction committed as.
   *
   * @return version the transaction is committed as.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Operations for connector to trigger post-commit.
   *
   * <p>Usage:
   *
   * <ul>
   *   <li>Async: Call {@link PostCommitHook#threadSafeInvoke(Engine)} in separate thread.
   *   <li>Sync: Direct call {@link PostCommitHook#threadSafeInvoke(Engine)} and block until
   *       operation ends.
   * </ul>
   *
   * @return list of post-commit operations
   */
  public List<PostCommitHook> getPostCommitHooks() {
    return postCommitHooks;
  }
}
