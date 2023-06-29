/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.actions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.delta.standalone.types.StructType;

/**
 * Updates the metadata of the table. The first version of a table must contain
 * a {@link Metadata} action. Subsequent {@link Metadata} actions completely
 * overwrite the current metadata of the table. It is the responsibility of the
 * writer to ensure that any data already present in the table is still valid
 * after any change. There can be at most one {@link Metadata} action in a
 * given version of the table.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata">Delta Transaction Log Protocol: Change Metadata</a>
 */
public final class Metadata implements Action {
    @Nonnull private final String id;
    @Nullable private final String name;
    @Nullable private final String description;
    @Nonnull private final Format format;
    @Nonnull private final List<String> partitionColumns;
    @Nonnull private final Map<String, String> configuration;
    @Nonnull private final Optional<Long> createdTime;
    @Nullable private final StructType schema;

    public Metadata(
            @Nonnull String id,
            @Nullable String name,
            @Nullable String description,
            @Nonnull Format format,
            @Nonnull List<String> partitionColumns,
            @Nonnull Map<String, String> configuration,
            @Nonnull Optional<Long> createdTime,
            @Nullable StructType schema) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.format = format;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.createdTime = createdTime;
        this.schema = schema;
    }

    /**
     * @return the unique identifier for this table
     */
    @Nonnull
    public String getId() {
        return id;
    }

    /**
     * @return the user-provided identifier for this table
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * @return the user-provided description for this table
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * @return the {@link Format} for this table
     */
    @Nonnull
    public Format getFormat() {
        return format;
    }

    /**
     * @return an unmodifiable {@code java.util.List} containing the names of
     *         columns by which the data should be partitioned
     */
    @Nonnull
    public List<String> getPartitionColumns() {
        return Collections.unmodifiableList(partitionColumns);
    }

    /**
     * @return an unmodifiable {@code java.util.Map} containing configuration
     *         options for this metadata
     */
    @Nonnull
    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    /**
     * @return the time when this metadata action was created, in milliseconds
     *         since the Unix epoch
     */
    @Nonnull
    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    /**
     * @return the schema of the table as a {@link StructType}
     */
    @Nullable
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(id, metadata.id) &&
                Objects.equals(name, metadata.name) &&
                Objects.equals(description, metadata.description) &&
                Objects.equals(format, metadata.format) &&
                Objects.equals(partitionColumns, metadata.partitionColumns) &&
                Objects.equals(configuration, metadata.configuration) &&
                Objects.equals(createdTime, metadata.createdTime) &&
                Objects.equals(schema, metadata.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, description, format, partitionColumns, configuration,
                createdTime, schema);
    }

    /**
     * @return a new {@link Metadata.Builder} initialized with the same properties as this
     *         {@link Metadata} instance
     */
    public Builder copyBuilder() {
        return new Builder(id, name, description, format, partitionColumns, configuration,
                createdTime, schema);
    }

    /**
     * @return a new {@link Metadata.Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link Metadata}. Enables construction of {@link Metadata}s with default
     * values.
     */
    public static final class Builder {
        @Nonnull private String id = java.util.UUID.randomUUID().toString();
        @Nullable private String name;
        @Nullable private String description;
        @Nonnull private Format format = new Format("parquet", Collections.emptyMap());
        @Nonnull private List<String> partitionColumns = Collections.emptyList();
        @Nonnull private Map<String, String> configuration = Collections.emptyMap();
        @Nonnull private Optional<Long> createdTime = Optional.of(System.currentTimeMillis());
        @Nullable private StructType schema;

        public Builder(){};

        private Builder(
                @Nonnull String id,
                @Nullable String name,
                @Nullable String description,
                @Nonnull Format format,
                @Nonnull List<String> partitionColumns,
                @Nonnull Map<String, String> configuration,
                @Nonnull Optional<Long> createdTime,
                @Nullable StructType schema) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.format = format;
            this.partitionColumns = partitionColumns;
            this.configuration = configuration;
            this.createdTime = createdTime;
            this.schema = schema;
        }

        public Builder id(@Nonnull String id) {
            this.id = id;
            return this;
        }

        public Builder name(@Nullable String name) {
            this.name = name;
            return this;
        }

        public Builder description(@Nullable String description) {
            this.description = description;
            return this;
        }

        public Builder format(@Nonnull Format format) {
            this.format = format;
            return this;
        }

        public Builder partitionColumns(@Nonnull List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder configuration(@Nonnull Map<String, String> configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder createdTime(Long createdTime) {
            this.createdTime = Optional.of(createdTime);
            return this;
        }

        public Builder createdTime(@Nonnull Optional<Long> createdTime) {
            this.createdTime = createdTime;
            return this;
        }

        public Builder schema(@Nullable StructType schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Builds a {@link Metadata} using the provided parameters. If a parameter is not provided
         * its default values is used.
         *
         * @return a new {@link Metadata} with the properties added to the builder
         */
        public Metadata build() {
            Metadata metadata = new Metadata(
                    this.id,
                    this.name,
                    this.description,
                    this.format,
                    this.partitionColumns,
                    this.configuration,
                    this.createdTime,
                    this.schema);
            return metadata;
        }
    }
}
