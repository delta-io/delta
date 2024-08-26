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
package shadedForDelta.org.apache.iceberg;

import org.junit.Assert;
import org.junit.Test;
import shadedForDelta.org.apache.iceberg.relocated.com.google.common.collect.Lists;
import shadedForDelta.org.apache.iceberg.types.Type.PrimitiveType;
import shadedForDelta.org.apache.iceberg.types.Types;
import shadedForDelta.org.apache.iceberg.types.Types.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static shadedForDelta.org.apache.iceberg.types.Types.NestedField.optional;
import static shadedForDelta.org.apache.iceberg.types.Types.NestedField.required;

public class TestEvolveSchemaVisitor {

    private static List<? extends PrimitiveType> primitiveTypes() {
        return Lists.newArrayList(
                StringType.get(),
                TimeType.get(),
                TimestampType.withoutZone(),
                TimestampType.withZone(),
                UUIDType.get(),
                DateType.get(),
                BooleanType.get(),
                BinaryType.get(),
                DoubleType.get(),
                IntegerType.get(),
                FixedType.ofLength(10),
                DecimalType.of(10, 2),
                LongType.get(),
                FloatType.get());
    }

    private static NestedField[] primitiveFields(
            Integer initialValue, List<? extends PrimitiveType> primitiveTypes) {
        AtomicInteger atomicInteger = new AtomicInteger(initialValue);
        return primitiveTypes.stream()
                .map(
                        type ->
                                optional(
                                        atomicInteger.incrementAndGet(),
                                        type.toString(),
                                        Types.fromPrimitiveString(type.toString())))
                .toArray(NestedField[]::new);
    }

    @Test
    public void testAddTopLevelPrimitives() {
        Schema targetSchema = new Schema(primitiveFields(0, primitiveTypes()));
        SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
        EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testDropTopLevelPrimitives() {
        Schema existingSchema = new Schema(primitiveFields(0, primitiveTypes()));
        SchemaUpdate updateApi = new SchemaUpdate(existingSchema, 0);
        EvolveSchemaVisitor.visit(updateApi, existingSchema, new Schema());
        Assert.assertEquals(updateApi.apply().asStruct().fields().size(), 0);
    }

    @Test
    public void testChangeOrderTopLevelPrimitives() {
        Schema existingSchema = new Schema(Arrays.asList(
                optional(1, "a", StringType.get()),
                optional(2, "b", StringType.get())
        ));
        Schema targetSchema = new Schema(Arrays.asList(
                optional(2, "b", StringType.get()),
                optional(1, "a", StringType.get())
        ));
        SchemaUpdate updateApi = new SchemaUpdate(existingSchema, 0);
        EvolveSchemaVisitor.visit(updateApi, existingSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }


    @Test
    public void testAddTopLevelListOfPrimitives() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema targetSchema =
                    new Schema(optional(1, "aList", ListType.ofOptional(2, primitiveType)));
            SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
            EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
        }
    }

    @Test
    public void testDropTopLevelListOfPrimitives() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema existingSchema = new Schema(optional(1, "aList", ListType.ofOptional(2, primitiveType)));
            Schema targetSchema = new Schema();
            SchemaUpdate updateApi = new SchemaUpdate(existingSchema, 0);
            EvolveSchemaVisitor.visit(updateApi, existingSchema, targetSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
        }
    }

    @Test
    public void testAddTopLevelMapOfPrimitives() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema targetSchema =
                    new Schema(
                            optional(1, "aMap", MapType.ofOptional(2, 3, primitiveType, primitiveType)));
            SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
            EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
        }
    }

    @Test
    public void testAddTopLevelStructOfPrimitives() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema currentSchema =
                    new Schema(
                            optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
            SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
            EvolveSchemaVisitor.visit(updateApi, new Schema(), currentSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), currentSchema.asStruct());
        }
    }

    @Test
    public void testAddNestedPrimitive() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema currentSchema = new Schema(optional(1, "aStruct", StructType.of()));
            Schema targetSchema =
                    new Schema(
                            optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
            SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 1);
            EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
        }
    }

    @Test
    public void testDropNestedPrimitive() {
        for (PrimitiveType primitiveType : primitiveTypes()) {
            Schema currentSchema =
                    new Schema(
                            optional(1, "aStruct", StructType.of(optional(2, "primitive", primitiveType))));
            Schema targetSchema = new Schema(optional(1, "aStruct", StructType.of()));
            SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 1);
            EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
            Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
        }
    }

    @Test
    public void testAddNestedPrimitives() {
        Schema currentSchema = new Schema(optional(1, "aStruct", StructType.of()));
        Schema targetSchema =
                new Schema(
                        optional(1, "aStruct", StructType.of(primitiveFields(1, primitiveTypes()))));
        SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 1);
        EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testAddNestedLists() {
        Schema targetSchema =
                new Schema(
                        optional(
                                1,
                                "aList",
                                ListType.ofOptional(
                                        2,
                                        ListType.ofOptional(
                                                3,
                                                ListType.ofOptional(
                                                        4,
                                                        ListType.ofOptional(
                                                                5,
                                                                ListType.ofOptional(
                                                                        6,
                                                                        ListType.ofOptional(
                                                                                7,
                                                                                ListType.ofOptional(
                                                                                        8,
                                                                                        ListType.ofOptional(
                                                                                                9,
                                                                                                ListType.ofOptional(
                                                                                                        10, DecimalType.of(11, 20))))))))))));
        SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
        EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testAddNestedStruct() {
        Schema targetSchema =
                new Schema(
                        optional(
                                1,
                                "struct1",
                                StructType.of(
                                        optional(
                                                2,
                                                "struct2",
                                                StructType.of(
                                                        optional(
                                                                3,
                                                                "struct3",
                                                                StructType.of(
                                                                        optional(
                                                                                4,
                                                                                "struct4",
                                                                                StructType.of(
                                                                                        optional(
                                                                                                5,
                                                                                                "struct5",
                                                                                                StructType.of(
                                                                                                        optional(
                                                                                                                6,
                                                                                                                "struct6",
                                                                                                                StructType.of(
                                                                                                                        optional(
                                                                                                                                7,
                                                                                                                                "aString",
                                                                                                                                StringType.get()))))))))))))));
        SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
        EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testAddNestedMaps() {
        Schema targetSchema =
                new Schema(
                        optional(
                                1,
                                "struct",
                                MapType.ofOptional(
                                        2,
                                        3,
                                        StringType.get(),
                                        MapType.ofOptional(
                                                4,
                                                5,
                                                StringType.get(),
                                                MapType.ofOptional(
                                                        6,
                                                        7,
                                                        StringType.get(),
                                                        MapType.ofOptional(
                                                                8,
                                                                9,
                                                                StringType.get(),
                                                                MapType.ofOptional(
                                                                        10,
                                                                        11,
                                                                        StringType.get(),
                                                                        MapType.ofOptional(
                                                                                12, 13, StringType.get(), StringType.get()))))))));
        SchemaUpdate updateApi = new SchemaUpdate(new Schema(), 0);
        EvolveSchemaVisitor.visit(updateApi, new Schema(), targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testDetectInvalidTopLevelList() {
        Schema currentSchema =
                new Schema(optional(1, "aList", ListType.ofOptional(2, StringType.get())));
        Schema targetSchema =
                new Schema(optional(1, "aList", ListType.ofOptional(2, LongType.get())));
        Assert.assertThrows(
                "Cannot change column type: aList.element: string -> long",
                IllegalArgumentException.class,
                () -> EvolveSchemaVisitor.visit(new SchemaUpdate(currentSchema, 2), currentSchema, targetSchema));
    }

    @Test
    public void testDetectInvalidTopLevelMapValue() {

        Schema currentSchema =
                new Schema(
                        optional(
                                1, "aMap", MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
        Schema targetSchema =
                new Schema(
                        optional(1, "aMap", MapType.ofOptional(2, 3, StringType.get(), LongType.get())));

        Assert.assertThrows(
                "Cannot change column type: aMap.value: string -> long",
                IllegalArgumentException.class,
                () -> EvolveSchemaVisitor.visit(new SchemaUpdate(currentSchema, 3), currentSchema, targetSchema));
    }

    @Test
    public void testDetectInvalidTopLevelMapKey() {
        Schema currentSchema =
                new Schema(
                        optional(
                                1, "aMap", MapType.ofOptional(2, 3, StringType.get(), StringType.get())));
        Schema targetSchema =
                new Schema(
                        optional(1, "aMap", MapType.ofOptional(2, 3, UUIDType.get(), StringType.get())));
        Assert.assertThrows(
                "Cannot change column type: aMap.key: string -> uuid",
                IllegalArgumentException.class,
                () -> EvolveSchemaVisitor.visit(new SchemaUpdate(currentSchema, 3), currentSchema, targetSchema));
    }

    @Test
    // int 32-bit signed integers -> Can promote to long
    public void testTypePromoteIntegerToLong() {
        Schema currentSchema = new Schema(required(1, "aCol", IntegerType.get()));
        Schema targetSchema = new Schema(required(1, "aCol", LongType.get()));

        SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 0);
        EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
        Schema applied = updateApi.apply();
        Assert.assertEquals(applied.asStruct().fields().size(), 1);
        Assert.assertEquals(applied.asStruct().fields().get(0).type(), LongType.get());
    }

    @Test
    // float 32-bit IEEE 754 floating point -> Can promote to double
    public void testTypePromoteFloatToDouble() {
        Schema currentSchema = new Schema(required(1, "aCol", FloatType.get()));
        Schema targetSchema = new Schema(required(1, "aCol", DoubleType.get()));

        SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 0);
        EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
        Schema applied = updateApi.apply();
        Assert.assertEquals(applied.asStruct().fields().size(), 1);
        Assert.assertEquals(applied.asStruct().fields().get(0).type(), DoubleType.get());
    }

    @Test
    public void testInvalidTypePromoteDoubleToFloat() {
        Schema currentSchema = new Schema(required(1, "aCol", DoubleType.get()));
        Schema targetSchema = new Schema(required(1, "aCol", FloatType.get()));
        Assert.assertThrows(
                "Cannot change column type: aCol: double -> float",
                IllegalArgumentException.class,
                () -> EvolveSchemaVisitor.visit(new SchemaUpdate(currentSchema, 3), currentSchema, targetSchema));
    }

    @Test
    // decimal(P,S) Fixed-point decimal; precision P, scale S -> Scale is fixed [1], precision must be
    // 38 or less
    public void testTypePromoteDecimalToFixedScaleWithWiderPrecision() {
        Schema currentSchema = new Schema(required(1, "aCol", DecimalType.of(20, 1)));
        Schema targetSchema = new Schema(required(1, "aCol", DecimalType.of(22, 1)));

        SchemaUpdate updateApi = new SchemaUpdate(currentSchema, 1);
        EvolveSchemaVisitor.visit(updateApi, currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testAddPrimitiveToNestedStruct() {
        Schema existingSchema =
                new Schema(
                        required(
                                1,
                                "struct1",
                                StructType.of(
                                        optional(
                                                2,
                                                "struct2",
                                                StructType.of(
                                                        optional(
                                                                3,
                                                                "list",
                                                                ListType.ofOptional(
                                                                        4,
                                                                        StructType.of(
                                                                                optional(5, "int", IntegerType.get())))))))));

        Schema targetSchema =
                new Schema(
                        required(
                                1,
                                "struct1",
                                StructType.of(
                                        optional(
                                                2,
                                                "struct2",
                                                StructType.of(
                                                        optional(
                                                                3,
                                                                "list",
                                                                ListType.ofOptional(
                                                                        4,
                                                                        StructType.of(
                                                                                optional(5, "long", LongType.get()),
                                                                                optional(6, "time", TimeType.get())))))))));

        SchemaUpdate updateApi = new SchemaUpdate(existingSchema, 5);
        EvolveSchemaVisitor.visit(updateApi, existingSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testReplaceListWithPrimitive() {
        Schema currentSchema =
                new Schema(optional(1, "aColumn", ListType.ofOptional(2, StringType.get())));
        Schema targetSchema = new Schema(optional(1, "aColumn", StringType.get()));
        Assert.assertThrows(
                "Cannot change column type: aColumn: list<string> -> string",
                IllegalArgumentException.class,
                () -> EvolveSchemaVisitor.visit(new SchemaUpdate(currentSchema, 3), currentSchema, targetSchema));
    }

    @Test
    public void addNewTopLevelStruct() {
        Schema currentSchema =
                new Schema(
                        optional(
                                1,
                                "map1",
                                MapType.ofOptional(
                                        2,
                                        3,
                                        StringType.get(),
                                        ListType.ofOptional(
                                                4, StructType.of(optional(5, "string1", StringType.get()))))));

        Schema targetSchema =
                new Schema(
                        optional(
                                1,
                                "map1",
                                MapType.ofOptional(
                                        2,
                                        3,
                                        StringType.get(),
                                        ListType.ofOptional(
                                                4, StructType.of(optional(5, "string1", StringType.get()))))),
                        optional(
                                6,
                                "struct1",
                                StructType.of(
                                        optional(
                                                7, "d1", StructType.of(optional(8, "d2", StringType.get()))))));

        SchemaUpdate updateApi = new SchemaUpdate( currentSchema, 5);
        EvolveSchemaVisitor.visit(updateApi,  currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testAppendNestedStruct() {
        Schema currentSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(
                                                                3, "s3", StructType.of(optional(4, "s4", StringType.get()))))))));

        Schema targetSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(3, "s3", StructType.of(optional(4, "s4", StringType.get()))),
                                                        optional(
                                                                5,
                                                                "repeat",
                                                                StructType.of(
                                                                        optional(
                                                                                6,
                                                                                "s1",
                                                                                StructType.of(
                                                                                        optional(
                                                                                                7,
                                                                                                "s2",
                                                                                                StructType.of(
                                                                                                        optional(
                                                                                                                8,
                                                                                                                "s3",
                                                                                                                StructType.of(
                                                                                                                        optional(
                                                                                                                                9,
                                                                                                                                "s4",
                                                                                                                                StringType.get()))))))))))))));

        SchemaUpdate updateApi = new SchemaUpdate( currentSchema, 4);
        EvolveSchemaVisitor.visit(updateApi,  currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testDropNestedStruct() {
        Schema currentSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(3, "s3", StructType.of(optional(4, "s4", StringType.get()))),
                                                        optional(
                                                                5,
                                                                "repeat",
                                                                StructType.of(
                                                                        optional(
                                                                                6,
                                                                                "s1",
                                                                                StructType.of(
                                                                                        optional(
                                                                                                7,
                                                                                                "s2",
                                                                                                StructType.of(
                                                                                                        optional(
                                                                                                                8,
                                                                                                                "s3",
                                                                                                                StructType.of(
                                                                                                                        optional(
                                                                                                                                9,
                                                                                                                                "s4",
                                                                                                                                StringType.get()))))))))))))));


        Schema targetSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(
                                                                3, "s3", StructType.of(optional(4, "s4", StringType.get()))))))));


        SchemaUpdate updateApi = new SchemaUpdate( currentSchema, 9);
        EvolveSchemaVisitor.visit(updateApi,  currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }

    @Test
    public void testRenameNestedList() {
        Schema currentSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(
                                                                3,
                                                                "s3",
                                                                StructType.of(
                                                                        optional(
                                                                                4,
                                                                                "list1",
                                                                                ListType.ofOptional(5, StringType.get())))))))));

        Schema targetSchema =
                new Schema(
                        required(
                                1,
                                "s1",
                                StructType.of(
                                        optional(
                                                2,
                                                "s2",
                                                StructType.of(
                                                        optional(
                                                                3,
                                                                "s3",
                                                                StructType.of(
                                                                        optional(
                                                                                4,
                                                                                "list2",
                                                                                ListType.ofOptional(5, StringType.get())))))))));

        SchemaUpdate updateApi = new SchemaUpdate( currentSchema, 5);
        EvolveSchemaVisitor.visit(updateApi,  currentSchema, targetSchema);
        Assert.assertEquals(updateApi.apply().asStruct(), targetSchema.asStruct());
    }
}
