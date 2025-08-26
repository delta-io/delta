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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.nio.file.Path;

/** Base class for all workload specifications. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ReadMetadata.class, name = "read_metadata")})
public abstract class WorkloadSpec {
  // The type of workload (e.g., "read_metadata") This is used by Jackson's polymorphic
  // deserialization to automatically instantiate the correct subclass based on the "type" field in
  // JSON.
  private String type;

  protected WorkloadSpec() {}

  protected WorkloadSpec(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public abstract WorkloadRunner getRunner(Path baseWorkloadDirPath);
}
