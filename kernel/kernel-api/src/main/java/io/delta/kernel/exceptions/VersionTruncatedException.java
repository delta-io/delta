/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;

/**
 * Thrown when the requested version to load is lower than the earliest version available in the
 * Delta log because the log has been truncated (e.g. by manual deletion or the log/checkpoint
 * retention policy). This is unrecoverable for the requested version.
 *
 * @since 4.3.0
 */
@Evolving
public class VersionTruncatedException extends KernelException {

  public VersionTruncatedException(String message) {
    super(message);
  }
}
