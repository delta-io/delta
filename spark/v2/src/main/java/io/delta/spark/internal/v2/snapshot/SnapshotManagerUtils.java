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
package io.delta.spark.internal.v2.snapshot;

import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import java.util.Map;
import java.util.Optional;

public final class SnapshotManagerUtils {

  private SnapshotManagerUtils() {}

  /**
   * Configures a create-table builder with optional table properties and data layout before
   * building the transaction.
   */
  public static Transaction configureAndBuildTransaction(
      CreateTableTransactionBuilder builder,
      Map<String, String> tableProperties,
      Optional<DataLayoutSpec> dataLayoutSpec,
      Engine kernelEngine) {
    if (!tableProperties.isEmpty()) {
      builder = builder.withTableProperties(tableProperties);
    }
    if (dataLayoutSpec.isPresent()) {
      builder = builder.withDataLayoutSpec(dataLayoutSpec.get());
    }
    return builder.build(kernelEngine);
  }
}
