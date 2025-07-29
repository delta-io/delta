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

package io.delta.kernel.internal.transaction;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

/**
 * As we transition from {@link io.delta.kernel.Transaction} (and {@link io.delta.kernel.Snapshot})
 * to {@link io.delta.kernel.transaction.TransactionV2} (and {@link io.delta.kernel.ResolvedTable}),
 * different parts of internal Kernel code will have to deal with both types of table "sources"
 * (Snapshot, ResolvedTable).
 *
 * <p>This is a common interface for the minimal set of methods that both sources will have to
 * expose.
 */
public interface TransactionDataSource {
  long getVersion();

  Protocol getProtocol();

  Metadata getMetadata();
}
