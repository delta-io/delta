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

package io.delta.storage.commit;

import java.util.Iterator;
import java.util.Map;

import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Exception raised by
 * {@link CommitCoordinatorClient#commit(LogStore, Configuration, TableDescriptor, long, Iterator, UpdatedActions)}
 *
 * <pre>
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 *  </pre>
 */
public class CommitFailedException extends Exception {

  private boolean retryable;

  private boolean conflict;

  private String message;

  public CommitFailedException(boolean retryable, boolean conflict, String message) {
    this.retryable = retryable;
    this.conflict = conflict;
    this.message = message;
  }

  public boolean getRetryable() {
    return retryable;
  }

  public boolean getConflict() {
    return conflict;
  }

  public String getMessage() {
    return message;
  }
}
