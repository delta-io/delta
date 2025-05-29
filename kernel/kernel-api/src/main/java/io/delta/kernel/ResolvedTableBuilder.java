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

package io.delta.kernel;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType;
import java.util.List;

/**
 * Builder for constructing a {@link ResolvedTable} instance.
 *
 * <p>This builder allows table managers (filesystems, catalogs) to provide any information they may
 * know about a Delta table and get back a {@link ResolvedTable}. When {@link #build(Engine)} is
 * invoked, Kernel will automatically fill any missing information needed to construct the {@link
 * ResolvedTable} by reading from the filesystem as needed.
 *
 * <p>If no version is specified, the builder will resolve to the latest version. Depending on the
 * {@link ParsedLogData} provided, Kernel can avoid expensive filesystem operations.
 */
@Experimental
public interface ResolvedTableBuilder {
  ResolvedTableBuilder atVersion(long version);

  // TODO: atTimestamp

  /** For now, only log datas of type {@link ParsedLogType#RATIFIED_STAGED_COMMIT}s are supported */
  ResolvedTableBuilder withLogData(List<ParsedLogData> logData);

  // TODO: withProtocolAndMetadata

  ResolvedTable build(Engine engine);
}
