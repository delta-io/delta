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

package io.delta.kernel.ccv2;

import io.delta.kernel.utils.FileStatus;
import java.util.List;

public interface CommitResult {

  String resultString();

  long getCommitAttemptVersion();

  interface Success extends CommitResult {

    @Override
    default String resultString() {
      return "Success";
    }
  }

  interface NonRetryableFailure extends CommitResult {

    @Override
    default String resultString() {
      return "NonRetryableFailure";
    }

    String getMessage();
  }

  interface RetryableFailure extends CommitResult {

    @Override
    default String resultString() {
      return "RetryableFailure";
    }

    String getMessage();

    // TODO: just call this catalog-registered commits? might be unbackfilled, might be backfilled,
    //       but we can all agree that they are registered in the catalog
    List<FileStatus> unbackfilledCommits();
  }
}
