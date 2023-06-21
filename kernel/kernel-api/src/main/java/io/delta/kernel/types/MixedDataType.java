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

import io.delta.kernel.data.ColumnVector;

/**
 * TODO:
 * This is a controversial data type to have, but we have no way to specify the schema
 * of JSON serialized table schema. In order to use the
 * {@link io.delta.kernel.client.JsonHandler#parseJson(ColumnVector, StructType)}, the Kernel
 * needs to specify the exact schema of the data, but depending upon the type serialized (`int` vs
 * `map`), the serialized JSON could have different schema.
 * <p>
 * `int` type column schema is serialized as:
 *
 *  <pre>
 *   {
 *     "name" : "a",
 *     "type" : "integer",
 *     "nullable" : false,
 *     "metadata" : { }
 *   }
 *  </pre>
 * <p>
 * `struct` type column schema is serialized as:
 * <pre>
 *   {
 *     "name" : "b",
 *     "type" : {
 *       "type" : "struct",
 *       "fields" : [ {
 *         "name" : "d",
 *         "type" : "integer",
 *         "nullable" : false,
 *         "metadata" : { }
 *       } ]
 *     },
 *     "nullable" : true,
 *     "metadata" : { }
 *   }
 * </pre>
 * Whenever this type is specified, reader should expect either a `string` value or `struct` value.
 * The implementation of reader should convert the `string` or `struct` value to `string` type.
 * Reader implementations can expect this type only for JSON format data reading cases only.
 */
public class MixedDataType extends DataType
{
    public static final MixedDataType INSTANCE = new MixedDataType();

    private MixedDataType() {}

    @Override
    public String toJson()
    {
        throw new UnsupportedOperationException(
            "this should never called as this type is not persisted to storage");
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj == this;
    }

    @Override
    public String toString()
    {
        return "mixed";
    }
}
