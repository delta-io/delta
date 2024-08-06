/*
 * Copyright (2023) The Delta Lake Project Authors.
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

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications which are licensed as specified above.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.types;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.kernel.internal.util.Preconditions;

/**
 * The metadata for a given {@link StructField}. The contents are immutable.
 */
public final class FieldMetadata {
    private final Map<String, Object> metadata;

    private FieldMetadata(Map<String, Object> metadata) {
        this.metadata = Collections.unmodifiableMap(metadata);
    }

    /**
     * @return list of the key-value pairs in this {@link FieldMetadata}
     */
    public Map<String, Object> getEntries() {
        return metadata;
    }

    /**
     * @param key  the key to check for
     * @return True if {@code this} contains a mapping for the given key, False otherwise
     */
    public boolean contains(String key) {
        return metadata.containsKey(key);
    }

    /**
     * @param key  the key to check for
     * @return the value to which the specified key is mapped, or null if there is no mapping for
     * the given key
     */
    public Object get(String key) {
        return metadata.get(key);
    }

    public Long getLong(String key) {
        return get(key, Long.class);
    }

    public Double getDouble(String key) {
        return get(key, Double.class);
    }

    public Boolean getBoolean(String key) {
        return get(key, Boolean.class);
    }

    public String getString(String key) {
        return get(key, String.class);
    }

    public FieldMetadata getMetadata(String key) {
        return get(key, FieldMetadata.class);
    }

    public Long[] getLongArray(String key) {
        return get(key, Long[].class);
    }

    public Double[] getDoubleArray(String key) {
        return get(key, Double[].class);
    }

    public Boolean[] getBooleanArray(String key) {
        return get(key, Boolean[].class);
    }

    public String[] getStringArray(String key) {
        return get(key, String[].class);
    }

    public FieldMetadata[] getMetadataArray(String key) {
        return get(key, FieldMetadata[].class);
    }

    public String toJson() {
        return metadata.entrySet().stream()
            .map(e -> String.format("\"%s\" : %s", e.getKey(), valueToJson(e.getValue())))
            .collect(Collectors.joining(",\n", "{", "}"));
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMetadata that = (FieldMetadata) o;
        if (this.metadata.size() != that.metadata.size()) return false;
        return this.metadata.entrySet().stream().allMatch(e ->
            Objects.equals(e.getValue(), that.metadata.get(e.getKey())) ||
                (e.getValue() != null && e.getValue().getClass().isArray() &&
                    that.metadata.get(e.getKey()).getClass().isArray() &&
                    Arrays.equals(
                        (Object[]) e.getValue(),
                        (Object[]) that.metadata.get(e.getKey()))));
    }

    @Override
    public int hashCode() {
        return metadata.entrySet()
            .stream()
            .mapToInt( entry -> (entry.getValue().getClass().isArray() ?
                (entry.getKey() == null ? 0 : entry.getKey().hashCode())^
                    Arrays.hashCode((Object[]) entry.getValue()) :
                entry.hashCode())
            ).sum();
    }

    /**
     * @return a new {@link FieldMetadata.Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return an empty {@link FieldMetadata} instance
     */
    public static FieldMetadata empty() {
        return builder().build();
    }

    /**
     * @param key  the key to check for
     * @param type the type to cast the value to
     * @return the value (casted to the given type) to which the specified key is mapped,
     * or null if there is no mapping for the given key
     */
    private <T> T get(String key, Class<T> type) {
        Object value = get(key);
        if (null == value) {
            return (T) value;
        }

        Preconditions.checkArgument(value.getClass().isAssignableFrom(type),
                "Expected '%s' to be of type '%s' but was '%s'", value, type, value.getClass());
        return type.cast(value);
    }

    // TODO this is a hack for now until we have real serialization as part of the JsonHandler
    /** Wraps string objects in quotations, otherwise converts using toString() */
    private String valueToJson(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return String.format("\"%s\"", value);
        } else {
            return value.toString();
        }
    }

    /**
     * Builder class for {@link FieldMetadata}.
     */
    public static class Builder {
        private Map<String, Object> metadata = new HashMap<String, Object>();

        public Builder putNull(String key) {
            metadata.put(key, null);
            return this;
        }

        public Builder putLong(String key, long value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putDouble(String key, double value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putBoolean(String key, boolean value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putString(String key, String value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putFieldMetadata(String key, FieldMetadata value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putLongArray(String key, Long[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putDoubleArray(String key, Double[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putBooleanArray(String key, Boolean[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putStringArray(String key, String[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putFieldMetadataArray(String key, FieldMetadata[] value) {
            metadata.put(key, value);
            return this;
        }

        /**
         * Adds all metadata from {@code meta.metadata} to the builder's {@code metadata}.
         * Entries in the builder's {@code metadata} are overwritten with the
         * entries from {@code meta.metadata}.
         * @param meta The {@link FieldMetadata} instance holding metadata
         * @return this
         */
        public Builder fromMetadata(FieldMetadata meta) {
            metadata.putAll(meta.metadata);
            return this;
        }

        /**
         * @return a new {@link FieldMetadata} with the mappings added to the builder
         */
        public FieldMetadata build() {
            return new FieldMetadata(this.metadata);
        }
    }
}
