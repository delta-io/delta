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

package io.delta.kernel.types;

import java.util.Locale;

public abstract class DataType {

    public static DataType createPrimitive(String typeName) {
        if (typeName.equals(IntegerType.INSTANCE.typeName())) return IntegerType.INSTANCE;
        if (typeName.equals(LongType.INSTANCE.typeName())) return LongType.INSTANCE;
        if (typeName.equals(StringType.INSTANCE.typeName())) return StringType.INSTANCE;
        if (typeName.equals(BooleanType.INSTANCE.typeName())) return BooleanType.INSTANCE;

        throw new IllegalArgumentException(
            String.format("Can't create primitive for type type %s", typeName)
        );
    }

    public String typeName() {
        String name = this.getClass().getSimpleName();
        if (name.endsWith("Type")) {
            name = name.substring(0, name.length() - 4);
        }
        return name.toLowerCase(Locale.ROOT);
    }
    public boolean equivalent(DataType dt) {
        return this.equals(dt);
    }

    @Override
    public String toString() {
        return typeName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType that = (DataType) o;
        return typeName().equals(that.typeName());
    }
}

