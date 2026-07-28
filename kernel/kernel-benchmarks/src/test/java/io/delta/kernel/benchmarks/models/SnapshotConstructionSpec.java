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

package io.delta.kernel.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.benchmarks.workloadrunners.SnapshotConstructionRunner;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;

public class SnapshotConstructionSpec extends WorkloadSpec {

  /** The snapshot version to read. If null, the latest version will be read. From spec file. */
  @JsonProperty("version")
  private Long version;

  // Default constructor for Jackson
  public SnapshotConstructionSpec() {
    super("snapshot_construction");
  }

  /** @return the snapshot version to read, or null if the latest version should be read. */
  public Long getVersion() {
    return version;
  }

  @Override
  public WorkloadRunner getRunner(Engine engine) {
    return new SnapshotConstructionRunner(this, engine);
  }
}
