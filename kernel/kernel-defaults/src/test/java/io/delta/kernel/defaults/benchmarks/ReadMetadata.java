/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.benchmarks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.delta.kernel.expressions.Predicate;

/**
 * Workload specification for read_metadata benchmarks. Contains fields specific to metadata reading
 * operations.
 */
public class ReadMetadata extends WorkloadSpec {

  @JsonProperty("table_root")
  private String tableRoot;

  @JsonProperty("snapshot_version")
  private Long snapshotVersion;

  @JsonProperty("predicate")
  @JsonDeserialize(using = PredicateDeserializer.class)
  private Predicate predicate;

  @JsonProperty("expected_scan_metadata")
  private String expectedScanMetadata;

  // Default constructor for Jackson
  public ReadMetadata() {
    super("read_metadata");
  }

  // Getters and setters
  public String getTableRoot() {
    return tableRoot;
  }

  public Long getSnapshotVersion() {
    return snapshotVersion;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  public String getExpectedScanMetadata() {
    return expectedScanMetadata;
  }

  @Override
  public String toString() {
    return "ReadMetadata{"
        + "tableRoot='"
        + tableRoot
        + '\''
        + ", snapshotVersion="
        + snapshotVersion
        + ", predicate="
        + predicate
        + ", expectedScanMetadata='"
        + expectedScanMetadata
        + '\''
        + '}';
  }
}
