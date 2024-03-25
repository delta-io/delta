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
package io.delta.kernel.defaults.internal.data.value;

import java.util.Arrays;

import io.delta.kernel.data.VariantValue;

/**
 * Default implementation of a Delta kernel VariantValue.
 */
public class DefaultVariantValue implements VariantValue {
    private final byte[] value;
    private final byte[] metadata;

    public DefaultVariantValue(byte[] value, byte[] metadata) {
        this.value = value;
        this.metadata = metadata;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public byte[] getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "VariantValue{value=" + Arrays.toString(value) +
            ", metadata=" + Arrays.toString(metadata) + '}';
    }

    /**
     * Compare two variants in bytes. The variant equality is more complex than it, and we haven't
     * supported it in the user surface yet. This method is only intended for tests.
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof DefaultVariantValue) {
            return Arrays.equals(value, ((DefaultVariantValue) other).getValue()) &&
                Arrays.equals(metadata, ((DefaultVariantValue) other).getMetadata());
        } else {
            return false;
        }
    }
}
