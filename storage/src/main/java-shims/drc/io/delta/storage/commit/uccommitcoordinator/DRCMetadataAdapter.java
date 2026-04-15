/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.unitycatalog.client.delta.DeltaModelUtils;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.TableMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Adapts a DRC {@link TableMetadata} (UC SDK model) to {@link AbstractMetadata}
 * (storage module interface). Zero-copy delegation -- reads directly from
 * the UC model object.
 *
 * <p>DRC-aware callers cast to this and call {@link #getDRCColumns()} for
 * direct UC model access (zero-roundtrip schema conversion to Spark StructType).
 * {@link #getSchemaString()} is provided as a safety net (lazy serialization)
 * for any caller that needs JSON.
 *
 * <p>Lives in java-shims/drc/ because it depends on UC SDK types.
 */
class DRCMetadataAdapter implements AbstractMetadata {
    private final TableMetadata delegate;
    private volatile String cachedSchemaJson;

    DRCMetadataAdapter(TableMetadata delegate) {
        this.delegate = delegate;
    }

    /** Direct access to UC model columns -- no JSON, no serialization. */
    public List<StructField> getDRCColumns() {
        return delegate.getColumns();
    }

    /** Lazy Delta-format JSON -- only serialized if someone actually calls this. */
    @Override
    public String getSchemaString() {
        if (cachedSchemaJson == null && delegate.getColumns() != null) {
            try {
                cachedSchemaJson = DeltaModelUtils.schemaToJson(delegate.getColumns());
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to serialize DRC schema to JSON", e);
            }
        }
        return cachedSchemaJson;
    }

    @Override
    public String getId() {
        return delegate.getTableUuid() != null
            ? delegate.getTableUuid().toString() : null;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public String getDescription() {
        return delegate.getComment();
    }

    @Override
    public String getProvider() {
        return "delta";
    }

    @Override
    public Map<String, String> getFormatOptions() {
        return Collections.emptyMap();
    }

    @Override
    public List<String> getPartitionColumns() {
        return delegate.getPartitionColumns();
    }

    @Override
    public Map<String, String> getConfiguration() {
        return delegate.getProperties();
    }

    @Override
    public Long getCreatedTime() {
        return null;
    }
}
