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
package io.delta.kernel.metrics;

import java.util.Optional;
import java.util.UUID;

/** Defines the common fields that are shared by reports for Delta operations */
public interface DeltaOperationReport extends MetricsReport {

  /** @return the path of the table */
  String getTablePath();

  /** @return a string representation of the operation this report is for */
  String getOperationType();

  /** @return a unique ID for this report */
  UUID getReportUUID();

  /** @return the exception thrown if this report is for a failed operation, otherwise empty */
  Optional<Exception> getException();
}
