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

package io.delta.storage.commit.actions;

/**
 * An interface representing a domain metadata action for a Delta table. Domain metadata actions
 * store configuration strings for named metadata domains.
 *
 * <p>Each action already encodes its intent:
 * <ul>
 *   <li>{@code removed == false}: upsert (set the domain to the given configuration)</li>
 *   <li>{@code removed == true}: tombstone (logically delete the domain)</li>
 * </ul>
 */
public interface AbstractDomainMetadata {

  /** A string used to identify a specific metadata domain. */
  String getDomain();

  /** A string containing configuration for the metadata domain. */
  String getConfiguration();

  /**
   * When {@code true}, this action serves as a tombstone to logically delete the metadata domain.
   */
  boolean isRemoved();
}
