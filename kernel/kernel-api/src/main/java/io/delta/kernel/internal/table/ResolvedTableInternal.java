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

package io.delta.kernel.internal.table;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Clock;
import java.util.Map;

/**
 * Internal extension of {@link ResolvedTable} that exposes additional interfaces to provides access
 * to information that are only needed by Kernel's internal operations but should not be exposed in
 * the public API.
 */
public interface ResolvedTableInternal extends ResolvedTable {
  String getLogPath();

  Protocol getProtocol();

  Metadata getMetadata();

  Clock getClock();

  Map<String, DomainMetadata> getActiveDomainMetadataMap();

  @VisibleForTesting
  LogSegment getLogSegment();

  @VisibleForTesting
  Lazy<LogSegment> getLazyLogSegment();
}
