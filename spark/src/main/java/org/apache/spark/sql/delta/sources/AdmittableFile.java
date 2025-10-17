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

package org.apache.spark.sql.delta.sources;

/**
 * Interface for files that can be admitted by admission control in Delta streaming sources.
 * This abstraction allows both Scala and Java IndexedFile implementations to be used with
 * the admission control logic.
 */
public interface AdmittableFile {
  /**
   * Returns true if this file has an associated file action (AddFile, RemoveFile, or CDCFile).
   * Placeholder IndexedFiles with no file action will return false.
   */
  boolean hasFileAction();

  /**
   * Returns the size of the file in bytes.
   * For files without a file action or files with unknown size, returns 0.
   */
  long getFileSize();
}
