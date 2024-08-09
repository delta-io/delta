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

import shadedForDelta.org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import shadedForDelta.org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import shadedForDelta.org.apache.iceberg.types.Type;
import shadedForDelta.org.apache.iceberg.types.Types;

import java.util.List;

/**
 * Visitor class that accumulates the set of changes needed to evolve an existing schema into the
 * target schema. Changes are applied to an {@link UpdateSchema} operation.
 */
public class EvolveSchemaVisitor extends SchemaWithPartnerVisitor<Integer, Boolean> {

    private final UpdateSchema api;
    private final Schema existingSchema;
    private final Schema targetSchema;

    private EvolveSchemaVisitor(UpdateSchema api, Schema existingSchema, Schema targetSchema) {
        this.api = api;
        this.existingSchema = existingSchema;
        this.targetSchema = targetSchema;
    }

    /**
     * Adds changes needed to produce the target schema to an {@link UpdateSchema} operation.
     *
     * <p>Changes are accumulated to evolve the existingSchema into a targetSchema.
     *
     * @param api an UpdateSchema for adding changes
     * @param existingSchema an existing schema
     * @param targetSchema a new schema to compare with the existing
     */
    public static void visit(UpdateSchema api, Schema existingSchema, Schema targetSchema) {
        visit(targetSchema,
                -1,
                new EvolveSchemaVisitor(api, existingSchema, targetSchema),
                new PartnerByIdAccessors(existingSchema));
    }

    @Override
    public Boolean struct(
            Types.StructType struct, Integer partnerId, List<Boolean> existingFields) {
        if (partnerId == null) {
            return true;
        }

        // Add, update and order fields in the struct
        Types.StructType partnerStruct = findFieldType(partnerId).asStructType();
        String after = null;
        for (Types.NestedField targetField : struct.fields()) {
            Types.NestedField nestedField = partnerStruct.field(targetField.fieldId());
            final String columnName;
            if (nestedField != null) {
                updateColumn(nestedField, targetField);
                // TODO: The move doesn't take the rename into account
                // https://github.com/apache/iceberg/issues/10830
                columnName = this.existingSchema.findColumnName(targetField.fieldId());
            } else {
                addColumn(partnerId, targetField);
                columnName = this.targetSchema.findColumnName(targetField.fieldId());
            }

            setPosition(columnName, after);
            after = columnName;
        }

        // Drop fields
        for (Types.NestedField existingField : partnerStruct.fields()) {
            int fieldId = existingField.fieldId();
            if (this.targetSchema.findField(fieldId) == null) {
                this.api.deleteColumn(this.existingSchema.findColumnName(fieldId));
            }
        }

        return false;
    }

    @Override
    public Boolean field(Types.NestedField field, Integer partnerId, Boolean isFieldMissing) {
        return partnerId == null;
    }

    @Override
    public Boolean list(Types.ListType list, Integer partnerId, Boolean isElementMissing) {
        if (partnerId == null) {
            return true;
        }

        Preconditions.checkState(
                !isElementMissing, "Error traversing schemas: element is missing, but list is present");

        Types.ListType partnerList = findFieldType(partnerId).asListType();
        updateColumn(partnerList.fields().get(0), list.fields().get(0));

        return false;
    }

    @Override
    public Boolean map(
            Types.MapType map, Integer partnerId, Boolean isKeyMissing, Boolean isValueMissing) {
        if (partnerId == null) {
            return true;
        }

        Preconditions.checkState(
                !isKeyMissing, "Error traversing schemas: key is missing, but map is present");
        Preconditions.checkState(
                !isValueMissing, "Error traversing schemas: value is missing, but map is present");

        Types.MapType partnerMap = findFieldType(partnerId).asMapType();
        updateColumn(partnerMap.fields().get(0), map.fields().get(0));
        updateColumn(partnerMap.fields().get(1), map.fields().get(1));

        return false;
    }

    @Override
    public Boolean primitive(Type.PrimitiveType primitive, Integer partnerId) {
        return partnerId == null;
    }

    private Type findFieldType(int fieldId) {
        if (fieldId == -1) {
            return existingSchema.asStruct();
        } else {
            return existingSchema.findField(fieldId).type();
        }
    }

    private void addColumn(int parentId, Types.NestedField field) {
        String parentName = targetSchema.findColumnName(parentId);
        api.addColumn(parentName, field.name(), field.type(), field.doc());
    }

    private void updateColumn(Types.NestedField existingField, Types.NestedField targetField) {
        String existingColumnName = this.existingSchema.findColumnName(existingField.fieldId());

        boolean needsNameUpdate = !targetField.name().equals(existingField.name());
        boolean needsOptionalUpdate = targetField.isOptional() && existingField.isRequired();
        boolean needsTypeUpdate =
                targetField.type().isPrimitiveType() && !targetField.type().equals(existingField.type());
        boolean needsDocUpdate = targetField.doc() != null && !targetField.doc().equals(existingField.doc());

        if (needsOptionalUpdate) {
            api.makeColumnOptional(existingColumnName);
        }

        if (needsTypeUpdate) {
            api.updateColumn(existingColumnName, targetField.type().asPrimitiveType());
        }

        if (needsDocUpdate) {
            api.updateColumnDoc(existingColumnName, targetField.doc());
        }

        if (needsNameUpdate) {
            api.renameColumn(existingColumnName, targetField.name());
        }
    }

    private void setPosition(String columnName, String after) {
        if (after == null) {
            this.api.moveFirst(columnName);
        } else {
            this.api.moveAfter(columnName, after);
        }
    }

    private static class PartnerByIdAccessors implements PartnerAccessors<Integer> {
        private final Schema partnerSchema;

        private PartnerByIdAccessors(Schema partnerSchema) {
            this.partnerSchema = partnerSchema;
        }

        @Override
        public Integer fieldPartner(Integer partnerFieldId, int fieldId, String name) {
            Types.StructType struct;
            if (partnerFieldId == -1) {
                struct = partnerSchema.asStruct();
            } else {
                struct = partnerSchema.findField(partnerFieldId).type().asStructType();
            }

            Types.NestedField field = struct.field(fieldId);
            if (field != null) {
                return field.fieldId();
            }

            return null;
        }

        @Override
        public Integer mapKeyPartner(Integer partnerMapId) {
            Types.NestedField mapField = partnerSchema.findField(partnerMapId);
            if (mapField != null) {
                return mapField.type().asMapType().fields().get(0).fieldId();
            }

            return null;
        }

        @Override
        public Integer mapValuePartner(Integer partnerMapId) {
            Types.NestedField mapField = partnerSchema.findField(partnerMapId);
            if (mapField != null) {
                return mapField.type().asMapType().fields().get(1).fieldId();
            }

            return null;
        }

        @Override
        public Integer listElementPartner(Integer partnerListId) {
            Types.NestedField listField = partnerSchema.findField(partnerListId);
            if (listField != null) {
                return listField.type().asListType().fields().get(0).fieldId();
            }

            return null;
        }
    }
}
