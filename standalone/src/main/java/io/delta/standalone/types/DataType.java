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

import java.util.Locale;
import java.util.Objects;

/**
 * The base type of all {@code io.delta.standalone} data types.
 * Represents a bare-bones Java implementation of the Spark SQL
 * <a href="https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DataType.scala" target="_blank">DataType</a>,
 * allowing Spark SQL schemas to be represented in Java.
 */
public abstract class DataType {

    /**
     * @return the name of the type used in JSON serialization
     */
    public String getTypeName() {
        String tmp = this.getClass().getSimpleName();
        tmp = stripSuffix(tmp, "$");
        tmp = stripSuffix(tmp, "Type");
        tmp = stripSuffix(tmp, "UDT");
        return tmp.toLowerCase(Locale.ROOT);
    }

    /**
     * @return a readable {@code String} representation for the type
     */
    public String getSimpleString() {
        return getTypeName();
    }

    /**
     * @return a {@code String} representation for the type saved in external catalogs
     */
    public String getCatalogString() {
        return getSimpleString();
    }

    /**
     * Builds a readable {@code String} representation of the {@code ArrayType}
     */
    protected static void buildFormattedString(
            DataType dataType,
            String prefix,
            StringBuilder builder) {
        if (dataType instanceof ArrayType) ((ArrayType) dataType).buildFormattedString(prefix, builder);
        if (dataType instanceof StructType) ((StructType) dataType).buildFormattedString(prefix, builder);
        if (dataType instanceof MapType) ((MapType) dataType).buildFormattedString(prefix, builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType that = (DataType) o;
        return getTypeName().equals(that.getTypeName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTypeName());
    }

    private String stripSuffix(String orig, String suffix) {
        if (null != orig && orig.endsWith(suffix)) {
            return orig.substring(0, orig.length() - suffix.length());
        }
        return orig;
    }
}
