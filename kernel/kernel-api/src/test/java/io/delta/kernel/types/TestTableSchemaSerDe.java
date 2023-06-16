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

import static io.delta.kernel.types.PrimitiveType.BINARY;
import static io.delta.kernel.types.PrimitiveType.BOOLEAN;
import static io.delta.kernel.types.PrimitiveType.BYTE;
import static io.delta.kernel.types.PrimitiveType.DATE;
import static io.delta.kernel.types.PrimitiveType.DOUBLE;
import static io.delta.kernel.types.PrimitiveType.FLOAT;
import static io.delta.kernel.types.PrimitiveType.INTEGER;
import static io.delta.kernel.types.PrimitiveType.LONG;
import static io.delta.kernel.types.PrimitiveType.SHORT;
import static io.delta.kernel.types.PrimitiveType.STRING;
import static io.delta.kernel.types.PrimitiveType.TIMESTAMP;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestTableSchemaSerDe
{
    @Test
    public void primitiveTypeRoundTrip()
    {
        List<DataType> primitiveTypeList = Arrays.asList(
            BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BINARY, STRING, DATE, TIMESTAMP);

        List<StructField> fieldList = new ArrayList<>();
        for (DataType dataType : primitiveTypeList) {
            fieldList.add(structField("col1" + dataType, dataType, true));
            fieldList.add(structField("col2" + dataType, dataType, false));
            fieldList.add(structField("col3" + dataType, dataType, false, sampleMetadata()));
        }

        fieldList.add(structField("col1decimal", new DecimalType(30, 10), true));
        fieldList.add(structField("col2decimal", new DecimalType(38, 22), false));
        fieldList.add(structField("col3decimal", new DecimalType(5, 2), false, sampleMetadata()));

        StructType expSchem = new StructType(fieldList);
        String serializedFormat = TableSchemaSerDe.toJson(expSchem);
        StructType actSchema = TableSchemaSerDe.fromJson(new JsonHandlerTestImpl(), serializedFormat);

        assertEquals(expSchem, actSchema);
    }

    @Test
    public void complexTypesRoundTrip()
    {
        List<StructField> fieldList = new ArrayList<>();

        ArrayType arrayType = array(INTEGER, true);
        ArrayType arrayArrayType = array(arrayType, false);
        MapType mapType = map(FLOAT, BINARY, false);
        MapType mapMapType = map(mapType, BINARY, true);
        StructType structType = new StructType()
            .add("simple", DATE);
        StructType structAllType = new StructType()
            .add("prim", BOOLEAN)
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
        StructType actSchema = TableSchemaSerDe.fromJson(new JsonHandlerTestImpl(), serializedFormat);

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

    private MapType map(DataType keyType, DataType valueType, boolean valueContainsNull) {
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