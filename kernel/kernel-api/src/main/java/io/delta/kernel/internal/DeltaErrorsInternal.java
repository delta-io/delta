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
package io.delta.kernel.internal;

/**
 * Contains methods to create developer-facing exceptions. See <a
 * href="https://github.com/delta-io/delta/blob/master/kernel/EXCEPTION_PRINCIPLES.md">Exception
 * Principles</a> for more information on what these are and how to use them.
 */
public class DeltaErrorsInternal {

  public static IllegalStateException missingRemoveFileSizeDuringCommit() {
    return new IllegalStateException(
        "Kernel APIs for creating remove file rows require that "
            + "file size be provided but found null file size");
  }
}
