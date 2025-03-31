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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.util.ColumnMapping.*;
import static io.delta.kernel.internal.util.ColumnMapping.getNestedColumnIds;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility methods for schema related operations such as validating the schema has no duplicate
 * columns and the names contain only valid characters.
 */
public class SchemaUtils {

  private SchemaUtils() {}

  /**
   * Validate the schema. This method checks if the schema has no duplicate columns, the names
   * contain only valid characters and the data types are supported.
   *
   * @param schema the schema to validate
   * @param isColumnMappingEnabled whether column mapping is enabled. When column mapping is
   *     enabled, the column names in the schema can contain special characters that are allowed as
   *     column names in the Parquet file
   * @throws IllegalArgumentException if the schema is invalid
   */
  public static void validateSchema(StructType schema, boolean isColumnMappingEnabled) {
    checkArgument(schema.length() > 0, "Schema should contain at least one column");

    List<String> flattenColNames = flattenNestedFieldNames(schema);

    // check there are no duplicate column names in the schema
    Set<String> uniqueColNames =
        flattenColNames.stream().map(String::toLowerCase).collect(Collectors.toSet());

    if (uniqueColNames.size() != flattenColNames.size()) {
      Set<String> uniqueCols = new HashSet<>();
      List<String> duplicateColumns =
          flattenColNames.stream()
              .map(String::toLowerCase)
              .filter(n -> !uniqueCols.add(n))
              .collect(Collectors.toList());
      throw DeltaErrors.duplicateColumnsInSchema(schema, duplicateColumns);
    }

    // Check the column names are valid
    if (!isColumnMappingEnabled) {
      validParquetColumnNames(flattenColNames);
    } else {
      // when column mapping is enabled, just check the name contains no new line in it.
      flattenColNames.forEach(
          name -> {
            if (name.contains("\\n")) {
              throw invalidColumnName(name, "\\n");
            }
          });
    }

    validateSupportedType(schema);
  }

  /**
   * Performs the following validations on an updated table schema using the current schema as a
   * base for validation. ColumnMapping must be enabled to call this
   *
   * <p>The following checks are performed:
   *
   * <ul>
   *   <li>No duplicate columns are allowed
   *   <li>Column names contain only valid characters
   *   <li>Data types are supported
   *   <li>Physical column name consistency is preserved in the new schema
   *   <li>ToDo: No new non-nullable fields are added or no tightening of nullable fields
   *   <li>ToDo: Nested IDs for array/map types are preserved in the new schema for IcebergCompatV2
   *   <li>ToDo: No type changes
   * </ul>
   */
  public static void validateUpdatedSchema(
      StructType currentSchema, StructType newSchema, Metadata metadata) {
    checkArgument(
        isColumnMappingModeEnabled(ColumnMapping.getColumnMappingMode(metadata.getConfiguration())),
        "Cannot validate updated schema when column mapping is disabled");
    validateSchema(newSchema, true /*columnMappingEnabled*/);
    validateSchemaEvolution(currentSchema, newSchema, metadata);
  }

  /**
   * Verify the partition columns exists in the table schema and a supported data type for a
   * partition column.
   *
   * @param schema
   * @param partitionCols
   */
  public static void validatePartitionColumns(StructType schema, List<String> partitionCols) {
    // partition columns are always the top-level columns
    Map<String, DataType> columnNameToType =
        schema.fields().stream()
            .collect(
                Collectors.toMap(
                    field -> field.getName().toLowerCase(Locale.ROOT), StructField::getDataType));

    partitionCols.stream()
        .forEach(
            partitionCol -> {
              DataType dataType = columnNameToType.get(partitionCol.toLowerCase(Locale.ROOT));
              checkArgument(
                  dataType != null, "Partition column %s not found in the schema", partitionCol);

              if (!(dataType instanceof BooleanType
                  || dataType instanceof ByteType
                  || dataType instanceof ShortType
                  || dataType instanceof IntegerType
                  || dataType instanceof LongType
                  || dataType instanceof FloatType
                  || dataType instanceof DoubleType
                  || dataType instanceof DecimalType
                  || dataType instanceof StringType
                  || dataType instanceof BinaryType
                  || dataType instanceof DateType
                  || dataType instanceof TimestampType
                  || dataType instanceof TimestampNTZType)) {
                throw unsupportedPartitionDataType(partitionCol, dataType);
              }
            });
  }

  /**
   * Delta expects partition column names to be same case preserving as the name in the schema. E.g:
   * Schema: (a INT, B STRING) and partition columns: (b). In this case we store the schema as (a
   * INT, B STRING) and partition columns as (B).
   *
   * <p>This method expects the inputs are already validated (i.e. schema contains all the partition
   * columns).
   */
  public static List<String> casePreservingPartitionColNames(
      StructType tableSchema, List<String> partitionColumns) {
    Map<String, String> columnNameMap = new HashMap<>();
    tableSchema
        .fieldNames()
        .forEach(colName -> columnNameMap.put(colName.toLowerCase(Locale.ROOT), colName));
    return partitionColumns.stream()
        .map(colName -> columnNameMap.get(colName.toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList());
  }

  /**
   * Convert the partition column names in {@code partitionValues} map into the same case as the
   * column in the table metadata. Delta expects the partition column names to preserve the case
   * same as the table schema.
   *
   * @param partitionColNames List of partition columns in the table metadata. The names preserve
   *     the case as given by the connector when the table is created.
   * @param partitionValues Map of partition column name to partition value. Convert the partition
   *     column name to be same case preserving name as its equivalent column in the {@code
   *     partitionColName}. Column name comparison is case-insensitive.
   * @return Rewritten {@code partitionValues} map with names case preserved.
   */
  public static Map<String, Literal> casePreservingPartitionColNames(
      List<String> partitionColNames, Map<String, Literal> partitionValues) {
    Map<String, String> partitionColNameMap = new HashMap<>();
    partitionColNames.forEach(
        colName -> partitionColNameMap.put(colName.toLowerCase(Locale.ROOT), colName));

    return partitionValues.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> partitionColNameMap.get(entry.getKey().toLowerCase(Locale.ROOT)),
                Map.Entry::getValue));
  }

  /**
   * Search (case-insensitive) for the given {@code colName} in the {@code schema} and return its
   * position in the {@code schema}.
   *
   * @param schema {@link StructType}
   * @param colName Name of the column whose index is needed.
   * @return Valid index or -1 if not found.
   */
  public static int findColIndex(StructType schema, String colName) {
    for (int i = 0; i < schema.length(); i++) {
      if (schema.at(i).getName().equalsIgnoreCase(colName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Finds `StructField`s that match a given check `f`. Returns the path to the column, and the
   * field of all fields that match the check.
   *
   * @param schema The DataType to filter
   * @param recurseIntoMapOrArrayElements This flag defines whether we should recurse into elements
   *     types of ArrayType and MapType.
   * @param f The function to check each StructField
   * @param stopOnFirstMatch If true, stop the search when the first match is found
   * @return A List of pairs, each containing a List of Strings (the path) and a StructField. If
   *     {@code stopOnFirstMatch} is true, the list will contain at most one element.
   */
  public static List<Tuple2<List<String>, StructField>> filterRecursively(
      DataType schema,
      boolean recurseIntoMapOrArrayElements,
      boolean stopOnFirstMatch,
      Function<StructField, Boolean> f) {
    return recurseIntoComplexTypes(
        schema, new ArrayList<>(), recurseIntoMapOrArrayElements, stopOnFirstMatch, f);
  }

  /**
   * Collects all leaf columns from the given schema (including flattened columns only for
   * StructTypes), up to maxColumns. NOTE: If maxColumns = -1, we collect ALL leaf columns in the
   * schema.
   */
  public static List<Column> collectLeafColumns(
      StructType schema, Set<String> excludedColumns, int maxColumns) {
    List<Column> result = new ArrayList<>();
    collectLeafColumnsInternal(schema, null, excludedColumns, result, maxColumns);
    return result;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Private methods                                                                           ///
  /////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *
   * <pre>
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   * </pre>
   */
  private static List<String> flattenNestedFieldNames(StructType schema) {
    List<Tuple2<List<String>, StructField>> columnPathToStructFields =
        filterRecursively(
            schema,
            true /* recurseIntoMapOrArrayElements */,
            false /* stopOnFirstMatch */,
            sf -> true);

    return columnPathToStructFields.stream()
        .map(t -> t._1)
        .map(SchemaUtils::concatWithDot)
        .collect(Collectors.toList());
  }

  private static List<Tuple2<List<String>, StructField>> recurseIntoComplexTypes(
      DataType type,
      List<String> columnPath,
      boolean recurseIntoMapOrArrayElements,
      boolean stopOnFirstMatch,
      Function<StructField, Boolean> f) {
    List<Tuple2<List<String>, StructField>> filtered = new ArrayList<>();

    if (type instanceof StructType) {
      StructType s = (StructType) type;
      for (StructField sf : s.fields()) {
        List<String> newColumnPath = new ArrayList<>(columnPath);
        newColumnPath.add(sf.getName());

        if (f.apply(sf)) {
          filtered.add(new Tuple2<>(newColumnPath, sf));
          if (stopOnFirstMatch) {
            return filtered;
          }
        }

        filtered.addAll(
            recurseIntoComplexTypes(
                sf.getDataType(),
                newColumnPath,
                recurseIntoMapOrArrayElements,
                stopOnFirstMatch,
                f));

        if (stopOnFirstMatch && !filtered.isEmpty()) {
          return filtered;
        }
      }
    } else if (type instanceof ArrayType && recurseIntoMapOrArrayElements) {
      ArrayType a = (ArrayType) type;
      List<String> newColumnPath = new ArrayList<>(columnPath);
      newColumnPath.add("element");
      return recurseIntoComplexTypes(
          a.getElementType(), newColumnPath, recurseIntoMapOrArrayElements, stopOnFirstMatch, f);
    } else if (type instanceof MapType && recurseIntoMapOrArrayElements) {
      MapType m = (MapType) type;
      List<String> keyColumnPath = new ArrayList<>(columnPath);
      keyColumnPath.add("key");
      List<String> valueColumnPath = new ArrayList<>(columnPath);
      valueColumnPath.add("value");
      filtered.addAll(
          recurseIntoComplexTypes(
              m.getKeyType(), keyColumnPath, recurseIntoMapOrArrayElements, stopOnFirstMatch, f));
      if (stopOnFirstMatch && !filtered.isEmpty()) {
        return filtered;
      }
      filtered.addAll(
          recurseIntoComplexTypes(
              m.getValueType(),
              valueColumnPath,
              recurseIntoMapOrArrayElements,
              stopOnFirstMatch,
              f));
    }

    return filtered;
  }

  /* Compute the SchemaChanges using field IDs */
  static SchemaChanges<StructFieldWithIds> computeSchemaChangesById(
      Map<Integer, StructFieldWithIds> currentFieldIdToField,
      Map<Integer, StructFieldWithIds> updatedFieldIdToField) {
    SchemaChanges.Builder<StructFieldWithIds> schemaDiff = SchemaChanges.builder();
    for (Map.Entry<Integer, StructFieldWithIds> fieldInUpdatedSchema :
        updatedFieldIdToField.entrySet()) {
      StructFieldWithIds existingField = currentFieldIdToField.get(fieldInUpdatedSchema.getKey());
      StructFieldWithIds updatedField = fieldInUpdatedSchema.getValue();
      // New field added
      if (existingField == null) {
        schemaDiff = schemaDiff.withAddedField(updatedField);
      } else if (existingField.field().equalsIgnoringNames(updatedField.field())) {
        // Field was just renamed
        schemaDiff = schemaDiff.withRenamedField(updatedField);
      } else {
        // Field changed nullability, metadata or type
        schemaDiff = schemaDiff.withUpdatedField(existingField, updatedField);
      }
    }

    for (Map.Entry<Integer, StructFieldWithIds> entry : currentFieldIdToField.entrySet()) {
      if (!updatedFieldIdToField.containsKey(entry.getKey())) {
        schemaDiff = schemaDiff.withRemovedField(entry.getValue());
      }
    }

    return schemaDiff.build();
  }

  private static void validatePhysicalNameConsistency(
      List<Tuple2<StructFieldWithIds, StructFieldWithIds>> updatedFields) {
    for (Tuple2<StructFieldWithIds, StructFieldWithIds> updatedField : updatedFields) {
      StructField currentField = updatedField._1.field();
      StructField newField = updatedField._2.field();
      if (!getPhysicalName(currentField).equals(getPhysicalName(newField))) {
        throw new IllegalArgumentException(
            String.format(
                "Existing field with id %s in current schema has "
                    + "physical name %s which is different from %s",
                getColumnId(currentField),
                getPhysicalName(currentField),
                getPhysicalName(newField)));
      }
    }
  }

  /* Validate if a given schema evolution is safe for a given column mapping mode*/
  private static void validateSchemaEvolution(
      StructType currentSchema, StructType newSchema, Metadata metadata) {
    ColumnMappingMode columnMappingMode =
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
    switch (columnMappingMode) {
      case ID:
      case NAME:
        validateSchemaEvolutionById(currentSchema, newSchema, metadata);
        return;
      case NONE:
        throw new UnsupportedOperationException(
            "Schema evolution without column mapping is not supported");
      default:
        throw new UnsupportedOperationException(
            "Unknown column mapping mode: " + columnMappingMode);
    }
  }

  /**
   * Validates a given schema evolution by using field ID as the source of truth for identifying
   * fields
   */
  private static void validateSchemaEvolutionById(
      StructType currentSchema, StructType newSchema, Metadata metadata) {
    boolean icebergCompatV2 = TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    Map<Integer, StructFieldWithIds> currentFieldsById = fieldsById(currentSchema, icebergCompatV2);
    Map<Integer, StructFieldWithIds> updatedFieldsById = fieldsById(newSchema, icebergCompatV2);
    SchemaChanges<StructFieldWithIds> schemaChanges =
        computeSchemaChangesById(currentFieldsById, updatedFieldsById);
    validatePhysicalNameConsistency(schemaChanges.updatedFields());
    validateNoInvalidFieldMoves(currentFieldsById, updatedFieldsById);
    // Validates that the updated schema does not contain breaking changes in terms of types and
    // nullability
    validateUpdatedSchemaCompatibility(schemaChanges, currentFieldsById, icebergCompatV2);
    // ToDo Potentially validate IcebergCompatV2 nested IDs
  }

  // Validates that no field is moved outside of its containing struct
  private static void validateNoInvalidFieldMoves(
      Map<Integer, StructFieldWithIds> currentFields, Map<Integer, StructFieldWithIds> newFields) {
    for (Map.Entry<Integer, StructFieldWithIds> entry : newFields.entrySet()) {
      StructFieldWithIds existing = currentFields.get(entry.getKey());
      // For top level columns, their parents will be null so that this equivalence check
      // generalizes
      if (existing != null && !Objects.equals(existing.parentId(), entry.getValue().parentId())) {
        throw new KernelException(
            String.format(
                "Cannot move field %s outside of its containing struct", existing.columnPath()));
      }
    }
  }

  /**
   * Verifies that no non-nullable fields are added, no existing field nullability is tightened and
   * no invalid type changes are performed
   */
  private static void validateUpdatedSchemaCompatibility(
      SchemaChanges<StructFieldWithIds> schemaChanges,
      Map<Integer, StructFieldWithIds> currentFields,
      boolean icebergCompatV2) {
    for (StructFieldWithIds addedField : schemaChanges.addedFields()) {
      if (!addedField.field().isNullable()) {
        throw new KernelException(
            String.format("Cannot add non-nullable field %s", addedField.field().getName()));
      }
    }

    for (Tuple2<StructFieldWithIds, StructFieldWithIds> updatedFields :
        schemaChanges.updatedFields()) {
      // In IcebergCompatV2, map keys where keys are structs cannot be modified beyond field renames
      if (icebergCompatV2) {
        StructFieldWithIds updatedField = updatedFields._2;
        if (updatedField.parentId() != null) {
          DataType parentDataType =
              currentFields.get(updatedField.parentId()).field().getDataType();
          // Check if a map key where the key is a struct has been modified
          // It's safe here to use columnPath to determine if the field is the map key instead of
          // the value because
          // we know that the parent is a map type and we know that the path internally assigned to
          // a map
          // key includes "key"
          if (updatedField.field().getDataType() instanceof StructType
              && parentDataType instanceof MapType
              && updatedField.columnPath().contains("key")) {
            throw new KernelException(
                "Cannot modify map field %s with struct key in icebergCompatV2");
          }
        }
      }

      // ToDo: See if recursion can be avoided by incorporating map key/value and array element
      // updates in updatedFields
      validateFieldCompatibility(updatedFields._1.field(), updatedFields._2.field());
    }
  }

  /**
   * Validate that there was no change in type from existing field from new field, excluding
   * modified, dropped, or added fields to structs. Validates that a field's nullability is not
   * tightened
   */
  private static void validateFieldCompatibility(StructField existingField, StructField newField) {
    if (existingField.isNullable() && !newField.isNullable()) {
      throw new KernelException(
          String.format(
              "Cannot tighten the nullability of existing field %s", existingField.getName()));
    }

    // Both fields are structs, ensure there's no changes in the individual fields
    if (existingField.getDataType() instanceof StructType
        && newField.getDataType() instanceof StructType) {
      StructType existingStruct = (StructType) existingField.getDataType();
      StructType newStruct = (StructType) newField.getDataType();
      Map<Integer, StructField> existingNestedFields =
          existingStruct.fields().stream()
              .collect(Collectors.toMap(ColumnMapping::getColumnId, Function.identity()));

      for (StructField newNestedField : newStruct.fields()) {
        StructField existingNestedField = existingNestedFields.get(getColumnId(newNestedField));
        if (existingNestedField != null) {
          validateFieldCompatibility(existingNestedField, newNestedField);
        }
      }
    } else if (existingField.getDataType() instanceof MapType
        && newField.getDataType() instanceof MapType) {
      MapType existingMapType = (MapType) existingField.getDataType();
      MapType newMapType = (MapType) newField.getDataType();

      validateFieldCompatibility(existingMapType.getKeyField(), newMapType.getKeyField());
      validateFieldCompatibility(existingMapType.getValueField(), newMapType.getValueField());
    } else if (existingField.getDataType() instanceof ArrayType
        && newField.getDataType() instanceof ArrayType) {
      ArrayType existingArrayType = (ArrayType) existingField.getDataType();
      ArrayType newArrayType = (ArrayType) newField.getDataType();

      validateFieldCompatibility(
          existingArrayType.getElementField(), newArrayType.getElementField());
    } else if (!existingField.getDataType().equivalent(newField.getDataType())) {
      throw new KernelException(
          String.format(
              "Cannot change the type of existing field %s from %s to %s",
              existingField.getName(), existingField.getDataType(), newField.getDataType()));
    }
  }

  /**
   * Returns a mapping of id to field wrappers containing the field, its id, its parent id and a
   * full column path. In the case of icebergCompatV2, for map key/value and array elements, the
   * parent id is the field ID of the map or array struct field, and the ID will be extracted from
   * nested column metadata.
   *
   * <p>For other cases, map keys/values and array elements, where these elements are not structs
   * will not be included in this mapping since those will not have IDs. When the elements are
   * structs, the parent ID will be the nearest containing struct field.
   *
   * @param schema
   * @param icebergCompatV2
   * @return
   */
  static Map<Integer, StructFieldWithIds> fieldsById(StructType schema, boolean icebergCompatV2) {
    Map<Integer, StructFieldWithIds> fieldsById = new HashMap<>();
    for (StructField field : schema.fields()) {
      validateFieldHasIdAndName(field);
      int columnId = getColumnId(field);
      checkArgument(
          !fieldsById.containsKey(columnId),
          "Field %s with id %s already exists",
          field.getName(),
          columnId);
      fieldsById.put(
          columnId, new StructFieldWithIds(field, getColumnId(field), null, field.getName()));
      idToFields(field, icebergCompatV2, fieldsById, null, getPhysicalName(field));
    }

    return fieldsById;
  }

  /**
   * StructFieldWrapper wraps a StructField along with its field ID, a parent ID, and the full
   * column path.
   */
  static class StructFieldWithIds implements Supplier<StructField> {
    private final StructField field;
    private final int id;
    private final Integer parentId;
    private final String columnPath;

    StructFieldWithIds(StructField field, int id, Integer parentId, String columnPath) {
      this.field = field;
      this.id = id;
      this.parentId = parentId;
      this.columnPath = columnPath;
    }

    public StructField field() {
      return field;
    }

    public int id() {
      return id;
    }

    public Integer parentId() {
      return parentId;
    }

    public String columnPath() {
      return columnPath;
    }

    @Override
    public StructField get() {
      return field;
    }
  }

  private static void idToFields(
      StructField field,
      boolean icebergCompatV2,
      Map<Integer, StructFieldWithIds> idToFields,
      Integer currentParent,
      String fieldPath) {
    // If the field is primitive, the mapping will already be populated
    if (field.getDataType() instanceof BasePrimitiveType) {
      return;
    }

    int nearestAncestorFieldId = hasColumnId(field) ? getColumnId(field) : currentParent;

    if (field.getDataType() instanceof StructType) {
      StructType structType = (StructType) field.getDataType();
      for (StructField nestedField : structType.fields()) {
        validateFieldHasIdAndName(nestedField);
        int columnId = getColumnId(nestedField);
        checkArgument(
            !idToFields.containsKey(columnId),
            "Field %s with id %s already exists",
            field.getName(),
            columnId);
        idToFields.put(
            columnId,
            new StructFieldWithIds(nestedField, columnId, nearestAncestorFieldId, fieldPath));
        idToFields(
            nestedField,
            icebergCompatV2,
            idToFields,
            nearestAncestorFieldId,
            fieldPath + "." + getPhysicalName(nestedField));
      }
    } else if (field.getDataType() instanceof MapType) {
      MapType mapType = (MapType) field.getDataType();
      int mapKeyId;
      int mapValueId;
      String keyPath = fieldPath + ".key";
      String valuePath = fieldPath + ".value";
      if (icebergCompatV2) {
        checkArgument(
            hasNestedColumnIds(field),
            "Map data types in icebergCompatV2 " +
                    "must have nested ID metadata defined for key and value");
        FieldMetadata nestedMetadata = getNestedColumnIds(field);
        mapKeyId = nestedMetadata.getLong(keyPath).intValue();
        mapValueId = nestedMetadata.getLong(valuePath).intValue();
        idToFields.put(
            mapKeyId,
            new StructFieldWithIds(
                mapType.getKeyField(), mapKeyId, nearestAncestorFieldId, keyPath));
        idToFields.put(
            mapValueId,
            new StructFieldWithIds(
                mapType.getValueField(), mapValueId, nearestAncestorFieldId, valuePath));
      } else {
        mapKeyId = nearestAncestorFieldId;
        mapValueId = nearestAncestorFieldId;
      }

      idToFields(mapType.getKeyField(), icebergCompatV2, idToFields, mapKeyId, keyPath);
      idToFields(mapType.getValueField(), icebergCompatV2, idToFields, mapValueId, valuePath);

    } else if (field.getDataType() instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) field.getDataType();
      int elementId;
      String elementPath = fieldPath + ".element";
      if (icebergCompatV2) {
        checkArgument(
            hasNestedColumnIds(field),
            "Array data types in icebergCompatV2 " +
                    "must have nested ID metadata defined for the array element");
        FieldMetadata nestedMetadata = getNestedColumnIds(field);
        elementId = nestedMetadata.getLong(elementPath).intValue();
        idToFields.put(
            elementId,
            new StructFieldWithIds(
                arrayType.getElementField(), elementId, nearestAncestorFieldId, elementPath));
      } else {
        elementId = nearestAncestorFieldId;
      }

      idToFields(arrayType.getElementField(), icebergCompatV2, idToFields, elementId, elementPath);
    }
  }

  private static void validateFieldHasIdAndName(StructField field) {
    checkArgument(hasColumnId(field), "Field %s is missing column id", field.getName());
    checkArgument(hasPhysicalName(field), "Field %s is missing physical name", field.getName());
  }

  /** column name by concatenating the column path elements (think of nested) with dots */
  private static String concatWithDot(List<String> columnPath) {
    return columnPath.stream().map(SchemaUtils::escapeDots).collect(Collectors.joining("."));
  }

  private static String escapeDots(String name) {
    return name.contains(".") ? "`" + name + "`" : name;
  }

  private static void collectLeafColumnsInternal(
      StructType schema,
      Column parentColumn,
      Set<String> excludedColumns,
      List<Column> result,
      int maxColumns) {
    boolean hasLimit = maxColumns != -1;
    for (StructField field : schema.fields()) {
      if (hasLimit && result.size() >= maxColumns) {
        return;
      }

      Column currentColumn = null;
      if (parentColumn == null) {
        // Skip excluded top-level columns
        if (excludedColumns.contains(field.getName())) {
          continue;
        }
        currentColumn = new Column(field.getName());
      } else {
        currentColumn = parentColumn.appendNestedField(field.getName());
      }

      if (field.getDataType() instanceof StructType) {
        collectLeafColumnsInternal(
            (StructType) field.getDataType(), currentColumn, excludedColumns, result, maxColumns);
      } else {
        result.add(currentColumn);
      }
    }
  }

  protected static void validParquetColumnNames(List<String> columnNames) {
    for (String name : columnNames) {
      // ,;{}()\n\t= and space are special characters in Parquet schema
      if (name.matches(".*[ ,;{}()\n\t=].*")) {
        throw invalidColumnName(name, "[ ,;{}()\\n\\t=]");
      }
    }
  }

  /**
   * Validate the supported data types. Once we start supporting additional types, take input the
   * protocol features and validate the schema.
   *
   * @param dataType the data type to validate
   */
  protected static void validateSupportedType(DataType dataType) {
    if (dataType instanceof BooleanType
        || dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType
        || dataType instanceof FloatType
        || dataType instanceof DoubleType
        || dataType instanceof DecimalType
        || dataType instanceof StringType
        || dataType instanceof BinaryType
        || dataType instanceof DateType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType) {
      // supported types
      return;
    } else if (dataType instanceof StructType) {
      ((StructType) dataType).fields().forEach(field -> validateSupportedType(field.getDataType()));
    } else if (dataType instanceof ArrayType) {
      validateSupportedType(((ArrayType) dataType).getElementType());
    } else if (dataType instanceof MapType) {
      validateSupportedType(((MapType) dataType).getKeyType());
      validateSupportedType(((MapType) dataType).getValueType());
    } else {
      throw unsupportedDataType(dataType);
    }
  }
}
