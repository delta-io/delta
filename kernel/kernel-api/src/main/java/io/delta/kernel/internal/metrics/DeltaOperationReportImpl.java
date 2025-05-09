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
package io.delta.kernel.internal.metrics;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.metrics.DeltaOperationReport;
import java.util.Optional;
import java.util.UUID;

/** Basic POJO implementation of {@link DeltaOperationReport} */
public abstract class DeltaOperationReportImpl implements DeltaOperationReport {

  private final String tablePath;
  private final UUID reportUUID;
  private final Optional<Exception> exception;

  protected DeltaOperationReportImpl(String tablePath, Optional<Exception> exception) {
    this.tablePath = requireNonNull(tablePath);
    this.reportUUID = UUID.randomUUID();
    this.exception = requireNonNull(exception);
  }

  @Override
  public String getTablePath() {
    return tablePath;
  }

  @Override
  public UUID getReportUUID() {
    return reportUUID;
  }

  @Override
  public Optional<Exception> getException() {
    return exception;
  }
}
