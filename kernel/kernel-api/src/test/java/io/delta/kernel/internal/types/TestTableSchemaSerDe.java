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
package io.delta.kernel.internal.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BasePrimitiveType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.types.JsonHandlerTestImpl;
import io.delta.kernel.internal.types.TableSchemaSerDe;

public class TestTableSchemaSerDe
{
    @Test
    public void primitiveTypeRoundTrip()
    {
        List<StructField> fieldList = new ArrayList<>();
        for (DataType dataType : BasePrimitiveType.getAllPrimitiveTypes()) {
            fieldList.add(structField("col1" + dataType, dataType, true));
            fieldList.add(structField("col2" + dataType, dataType, false));
            fieldList.add(structField("col3" + dataType, dataType, false, sampleMetadata()));
        }

        fieldList.add(structField("col1decimal", new DecimalType(30, 10), true));
        fieldList.add(structField("col2decimal", new DecimalType(38, 22), false));
        fieldList.add(structField("col3decimal", new DecimalType(5, 2), false, sampleMetadata()));

        StructType expSchem = new StructType(fieldList);
        String serializedFormat = TableSchemaSerDe.toJson(expSchem);
        StructType actSchema =
            TableSchemaSerDe.fromJson(new JsonHandlerTestImpl(), serializedFormat);

        assertEquals(expSchem, actSchema);
    }

    @Test
    public void complexTypesRoundTrip()
    {
        List<StructField> fieldList = new ArrayList<>();

        ArrayType arrayType = array(IntegerType.INSTANCE, true);
        ArrayType arrayArrayType = array(arrayType, false);
        MapType mapType = map(FloatType.INSTANCE, BinaryType.INSTANCE, false);
        MapType mapMapType = map(mapType, BinaryType.INSTANCE, true);
        StructType structType = new StructType()
            .add("simple", DateType.INSTANCE);
        StructType structAllType = new StructType()
            .add("prim", BooleanType.INSTANCE)
            .add("arr", arrayType)
            .add("map", mapType)
            .add("struct", structType);

        fieldList.add(structField("col1", arrayType, true));
        fieldList.add(structField("col2", arrayArrayType, false));
        fieldList.add(structField("col3", mapType, false));
        fieldList.add(structField("col4", mapMapType, false));
        fieldList.add(structField("col5", structType, false));
        fieldList.add(structField("col6", structAllType, false));

        StructType expSchem = new StructType(fieldList);
        String serializedFormat = TableSchemaSerDe.toJson(expSchem);
        StructType actSchema =
            TableSchemaSerDe.fromJson(new JsonHandlerTestImpl(), serializedFormat);

        assertEquals(expSchem, actSchema);
    }

    private StructField structField(String name, DataType type, boolean nullable)
    {
        return structField(name, type, nullable, Collections.emptyMap());
    }

    private StructField structField(
        String name,
        DataType type,
        boolean nullable,
        Map<String, String> metadata)
    {
        return new StructField(name, type, nullable, metadata);
    }

    private ArrayType array(DataType elemType, boolean containsNull)
    {
        return new ArrayType(elemType, containsNull);
    }

    private MapType map(DataType keyType, DataType valueType, boolean valueContainsNull)
    {
        return new MapType(keyType, valueType, valueContainsNull);
    }

    private Map<String, String> sampleMetadata()
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        return metadata;
    }
}
