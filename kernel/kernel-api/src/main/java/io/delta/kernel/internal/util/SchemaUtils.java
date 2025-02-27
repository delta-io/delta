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
   *   <li>No new non-nullable fields are added or no tightening of nullable fields
   *   <li>Physical column name consistency is preserved in the new schema
   *   <li>Partition columns are not modified
   *   <li>Nested IDs for array/map types are preserved in the new schema
   *   <li>No type changes
   * </ul>
   */
  public static void validateUpdatedSchema(
      StructType currentSchema,
      StructType newSchema,
      Set<String> expectedPartitionColumns,
      Metadata metadata) {
    checkArgument(
        isColumnMappingModeEnabled(
            ColumnMapping.getColumnMappingMode(metadata.getConfiguration())));
    validateSchema(newSchema, true /*columnMappingEnabled*/);
    validatePartitionColumns(newSchema, new ArrayList<>(expectedPartitionColumns));
    validateSchemaEvolution(
        currentSchema,
        newSchema,
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration()),
        metadata);
  }

  /* Validate if a given schema evolution is safe for a given column mapping mode*/
  private static void validateSchemaEvolution(
      StructType currentSchema,
      StructType newSchema,
      ColumnMappingMode columnMappingMode,
      Metadata metadata) {
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

  /* Validates a given schema evolution by using field ID
  as the source of truth for identifying fields */
  private static void validateSchemaEvolutionById(
      StructType currentSchema, StructType newSchema, Metadata metadata) {
    Map<Long, StructField> currentFieldsById = fieldsById(columnPathToField(currentSchema));
    Map<Long, StructField> updatedFieldsById = fieldsById(columnPathToField(newSchema));
    SchemaChanges schemaChanges = computeSchemaDiffById(currentFieldsById, updatedFieldsById);

    // Validate physical name consistency on the updated fields
    validatePhysicalNameConsistency(schemaChanges.updatedFields());

    // If IcebergCompatV2 validate nested ID existence and consistency for relevant fields
    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata)) {
      validateNestedIds(schemaChanges, currentFieldsById, updatedFieldsById);
    }

    // Validate the schema against the diff to make sure there's no type or nullability changes
    validateUpdatedSchemaCompatibility(schemaChanges);
  }

  /* Compute the SchemaChanges using field IDs */
  private static SchemaChanges computeSchemaDiffById(
      Map<Long, StructField> currentFieldIdToField, Map<Long, StructField> updatedFieldIdToField) {
    SchemaChanges.Builder schemaDiff = SchemaChanges.builder();
    for (Map.Entry<Long, StructField> fieldInUpdatedSchema : updatedFieldIdToField.entrySet()) {
      StructField existingField = currentFieldIdToField.get(fieldInUpdatedSchema.getKey());
      StructField updatedField = fieldInUpdatedSchema.getValue();
      // New field added
      if (existingField == null) {
        schemaDiff.withAddedField(updatedField);
      } else if (isUpdatedField(existingField, fieldInUpdatedSchema.getValue())) {
        // Field renamed or changed types
        schemaDiff.withUpdatedField(existingField, fieldInUpdatedSchema.getValue());
      }
    }

    for (Map.Entry<Long, StructField> entry : currentFieldIdToField.entrySet()) {
      StructField fieldInUpdatedSchema = updatedFieldIdToField.get(entry.getKey());
      if (fieldInUpdatedSchema == null) {
        schemaDiff.withRemovedField(entry.getValue());
      }
    }

    return schemaDiff.build();
  }

  private static void validatePhysicalNameConsistency(
      List<Tuple2<StructField, StructField>> updatedFields) {
    for (Tuple2<StructField, StructField> updatedField : updatedFields) {
      StructField currentField = updatedField._1;
      StructField newField = updatedField._2;
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

  private static void validateNestedIds(
      SchemaChanges schemaChanges,
      Map<Long, StructField> currentFieldsById,
      Map<Long, StructField> updatedFieldsById) {
    for (Tuple2<StructField, StructField> updatedField : schemaChanges.updatedFields()) {
      validateNestedIdsExist(updatedField._2);
    }

    for (StructField addedField : schemaChanges.addedFields()) {
      validateNestedIdsExist(addedField);
    }

    validateNestedIdConsistency(currentFieldsById, updatedFieldsById);
  }

  /**
   * Validate that any nested IDs that were in the current field are consistent with the ones in the
   * updated field and that the nested IDs are not duplicate
   */
  private static void validateNestedIdConsistency(
      Map<Long, StructField> currentFieldsById, Map<Long, StructField> updatedFieldsById) {
    Map<Long, List<NestedIdEntry>> currentFieldIdToNestedIds =
        columnIdToNestedIds(currentFieldsById);
    Map<Long, List<NestedIdEntry>> updatedFieldIdToNestedIds =
        columnIdToNestedIds(updatedFieldsById);
    /* Validate that nested field IDs exist in the schema
    and are consistent with the previous schemas */
    for (Map.Entry<Long, List<NestedIdEntry>> entry : updatedFieldIdToNestedIds.entrySet()) {
      Long fieldId = entry.getKey();
      List<NestedIdEntry> updatedNestedIdEntries = entry.getValue();
      Set<Long> seenNestedIds = new HashSet<>();
      for (NestedIdEntry nestedIdEntry : updatedNestedIdEntries) {
        // Duplicate nested ID with top level field
        if (updatedFieldIdToNestedIds.containsKey(nestedIdEntry.id)) {
          throw new IllegalArgumentException(
              String.format("Duplicate field id %s", nestedIdEntry.id));
        } else if (!seenNestedIds.add(nestedIdEntry.id)) {
          throw new IllegalArgumentException(
              String.format("Duplicate field id %s", nestedIdEntry.id));
        }
      }

      List<NestedIdEntry> existingNestedIdEntries = currentFieldIdToNestedIds.get(fieldId);

      /* If this is not a new column make sure
      the existing nested IDs are consistent in the updated field */
      if (existingNestedIdEntries != null) {
        // Group new ID entries for this field by key and compare
        Map<String, Long> updatedNestedKeyToColumnId = new HashMap<>();
        for (NestedIdEntry updatedNestedIdEntry : updatedNestedIdEntries) {
          updatedNestedKeyToColumnId.put(updatedNestedIdEntry.key, updatedNestedIdEntry.id);
        }

        for (NestedIdEntry existingNestedIdEntry : existingNestedIdEntries) {
          Long updatedNestedColumnId = updatedNestedKeyToColumnId.get(existingNestedIdEntry.key);
          if (updatedNestedColumnId == null
              || !updatedNestedColumnId.equals(existingNestedIdEntry.id)) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected field with id %s to have nested key %s",
                    fieldId, existingNestedIdEntry.key));
          }
        }
      }
    }
  }

  private static void validateNestedIdsExist(StructField field) {
    if (!hasNestedColumnIds(field)) {
      if (field.getDataType() instanceof MapType) {
        throw new IllegalArgumentException(
            String.format("Map field %s must have at least 2 nested field ids", field.getName()));
      } else if (field.getDataType() instanceof ArrayType) {
        throw new IllegalArgumentException(
            String.format("Array field %s must have at least 1 nested field id", field.getName()));
      }
    } else {
      if (field.getDataType() instanceof MapType && getNestedFieldIds(field).size() < 2) {
        throw new IllegalArgumentException(
            String.format("Map field %s must have at least 2 nested field ids", field.getName()));
      }
    }
  }

  /* Mapping of field ID to nested IDs for that field */
  private static Map<Long, List<NestedIdEntry>> columnIdToNestedIds(
      Map<Long, StructField> currentFieldIdToField) {
    Map<Long, List<NestedIdEntry>> nestedIdEntries = new HashMap<>();
    for (Map.Entry<Long, StructField> entry : currentFieldIdToField.entrySet()) {
      if (hasNestedColumnIds(entry.getValue())) {
        nestedIdEntries.put(entry.getKey(), getNestedFieldIds(entry.getValue()));
      }
    }

    return nestedIdEntries;
  }

  private static List<NestedIdEntry> getNestedFieldIds(StructField field) {
    return getNestedColumnIds(field).getEntries().entrySet().stream()
        .map(
            nestedIdEntry ->
                new NestedIdEntry(nestedIdEntry.getKey(), (Long) nestedIdEntry.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Wrapper object for icebergCompatV2 nested ID entry to simplify validation e.g. a schema with
   * col1 as map[int, array[int]] could look like <key: "col1.key", id: 1> <key: "col1.value", id:
   * 2> <key: "col1.value.element", id: 3>
   */
  private static class NestedIdEntry {
    private final String key;
    private final long id;

    NestedIdEntry(String key, long id) {
      this.key = key;
      this.id = id;
    }
  }

  // Returns if the field has been renamed, if the types are different or if fields have been moved
  private static boolean isUpdatedField(StructField existingField, StructField updatedField) {
    return !existingField.getName().equals(updatedField.getName())
        || !existingField.equals(updatedField);
  }

  /*Returns fields by ID, failing if either the column ID or physical name is missing*/
  private static Map<Long, StructField> fieldsById(Map<String, StructField> columnPathToField) {
    Map<Long, StructField> columnIdToField = new HashMap<>();
    for (Map.Entry<String, StructField> entry : columnPathToField.entrySet()) {
      StructField field = entry.getValue();
      if (!hasColumnId(field)) {
        throw new IllegalArgumentException(
            String.format("Field %s is missing column id", field.getName()));
      }

      if (!hasPhysicalName(field)) {
        throw new IllegalArgumentException(
            String.format("Field %s is missing physical name", field.getName()));
      }

      Long columnId = field.getMetadata().getLong(COLUMN_MAPPING_ID_KEY);
      if (columnIdToField.containsKey(columnId)) {
        throw new IllegalArgumentException(
            String.format("Field %s with id %d already exists", field.getName(), columnId));
      }

      columnIdToField.put(columnId, field);
    }

    return columnIdToField;
  }

  /* Verifies that no non-nullable fields are added, no type changes are performed and that
  partition columns are not modified */
  private static void validateUpdatedSchemaCompatibility(SchemaChanges diff) {
    for (StructField addedField : diff.addedFields()) {
      if (!addedField.isNullable()) {
        throw new KernelException(
            String.format("Cannot add a non-nullable field %s", addedField.getName()));
      }
    }

    for (Tuple2<StructField, StructField> updatedFields : diff.updatedFields()) {
      validateNoTypeChange(updatedFields._1, updatedFields._2);
    }
  }

  /**
   * Validate that there was no change in type from existing field from new field, excluding dropped
   * columns for structs. Note, fields are resolved by case-sensitive name
   */
  private static void validateNoTypeChange(StructField existingField, StructField newField) {
    if (existingField.isNullable() && !newField.isNullable()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot tighten the nullability of existing field %s", existingField.getName()));
    }

    // Both fields are structs, ensure there's no changes in the individual fields
    if (existingField.getDataType() instanceof StructType
        && newField.getDataType() instanceof StructType) {
      StructType existingStruct = (StructType) existingField.getDataType();
      Map<String, List<StructField>> existingFieldsByName =
          existingStruct.fields().stream().collect(Collectors.groupingBy(StructField::getName));
      StructType newStruct = (StructType) newField.getDataType();
      for (StructField newNestedField : newStruct.fields()) {
        List<StructField> existingNestedField = existingFieldsByName.get(newNestedField.getName());
        // The existing field is guaranteed to be well-defined, there should only be one field with
        // the name
        if (existingNestedField != null) {
          validateNoTypeChange(existingNestedField.get(0), newNestedField);
        }
      }
    } else if (existingField.getDataType() instanceof MapType
        && newField.getDataType() instanceof MapType) {
      MapType existingMapType = (MapType) existingField.getDataType();
      MapType newMapType = (MapType) newField.getDataType();

      validateNoTypeChange(existingMapType.getKeyField(), newMapType.getKeyField());
      validateNoTypeChange(existingMapType.getValueField(), newMapType.getValueField());
    } else if (existingField.getDataType() instanceof ArrayType
        && newField.getDataType() instanceof ArrayType) {
      ArrayType existingArrayType = (ArrayType) existingField.getDataType();
      ArrayType newArrayType = (ArrayType) newField.getDataType();

      validateNoTypeChange(existingArrayType.getElementField(), newArrayType.getElementField());
    } else if (!existingField.getDataType().equivalent(newField.getDataType())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot change the type of existing field %s from %s to %s",
              existingField.getName(), existingField.getDataType(), newField.getDataType()));
    }
  }

  /** Returns a map from flattened column path to the field in the schema */
  private static Map<String, StructField> columnPathToField(StructType schema) {
    List<Tuple2<List<String>, StructField>> columnPathToStructField =
        filterRecursively(
            schema,
            true /* recurseIntoMapOrArrayElements */,
            false /* stopOnFirstMatch */,
            sf -> true);
    Map<String, StructField> columnPathToStructFieldMap = new HashMap<>();
    for (Tuple2<List<String>, StructField> path : columnPathToStructField) {
      columnPathToStructFieldMap.put(SchemaUtils.concatWithDot(path._1), path._2);
    }

    return columnPathToStructFieldMap;
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
