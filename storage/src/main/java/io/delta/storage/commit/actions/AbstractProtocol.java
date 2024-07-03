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

package io.delta.storage.commit.actions;

import java.util.Set;

/**
 * Interface for protocol actions in Delta. The protocol defines the requirements
 * that readers and writers of the table need to meet.
 */
public interface AbstractProtocol {

  /**
   * The minimum reader version required to read the table.
   */
  int getMinReaderVersion();

  /**
   * The minimum writer version required to read the table.
   */
  int getMinWriterVersion();

  /**
   * The reader features that need to be supported to read the table.
   */
  Set<String> getReaderFeatures();

  /**
   * The writer features that need to be supported to write the table.
   */
  Set<String> getWriterFeatures();
}
