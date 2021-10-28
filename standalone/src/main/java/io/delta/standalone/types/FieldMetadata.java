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

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.types;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The metadata for a given {@link StructField}.
 */
public final class FieldMetadata {
    private final Map<String, Object> metadata;

    private FieldMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * @return list of the key-value pairs in {@code this}.
     */
    public Map<String, Object> getEntries() {
        return Collections.unmodifiableMap(metadata);
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

    @Override
    public String toString() {
        return metadata.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" +
                        (entry.getValue().getClass().isArray() ?
                                Arrays.toString((Object[]) entry.getValue()) :
                                entry.getValue().toString()))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMetadata that = (FieldMetadata) o;
        if (this.metadata.size() != that.metadata.size()) return false;
        return this.metadata.entrySet().stream().allMatch(e ->
                e.getValue().equals(that.metadata.get(e.getKey())) ||
                        (e.getValue().getClass().isArray() &&
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
     * @return a new {@code FieldMetadata.Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for FieldMetadata.
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

        public Builder putMetadata(String key, FieldMetadata value) {
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

        public Builder putMetadataArray(String key, FieldMetadata[] value) {
            metadata.put(key, value);
            return this;
        }

        /**
         * @return a new {@code FieldMetadata} with the same mappings as {@code this}
         */
        public FieldMetadata build() {
            return new FieldMetadata(this.metadata);
        }
    }
}
