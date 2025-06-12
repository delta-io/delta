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
import static io.delta.kernel.internal.util.ColumnMapping.getColumnId;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.internal.types.TypeWideningChecker;
import io.delta.kernel.types.*;
import java.util.*;
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

    List<String> flattenColNames =
        new SchemaIterable(schema)
            .stream().map(SchemaIterable.SchemaElement::getNamePath).collect(Collectors.toList());

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
   * base for validation. ColumnMapping must be enabled to call this.
   *
   * <p>Returns an updated schema if metadata (i.e. TypeChanges needs to be copied over from
   * currentSchema and new type changes need to be recorded. Kernel is expected to handle this work
   * instead of clients).
   *
   * <p>The following checks are performed:
   *
   * <ul>
   *   <li>No duplicate columns are allowed
   *   <li>Column names contain only valid characters
   *   <li>Data types are supported
   *   <li>Physical column name consistency is preserved in the new schema
   *   <li>If IcebergWriterCompatV1 is enabled, that map struct keys have not changed
   *   <li>ToDo: Nested IDs for array/map types are preserved in the new schema for IcebergCompatV2
   * </ul>
   */
  public static Optional<StructType> validateUpdatedSchemaAndGetUpdatedSchema(
      Metadata currentMetadata,
      Metadata newMetadata,
      Set<String> clusteringColumnPhysicalNames,
      boolean allowNewRequiredFields) {
    checkArgument(
        isColumnMappingModeEnabled(
            ColumnMapping.getColumnMappingMode(newMetadata.getConfiguration())),
        "Cannot validate updated schema when column mapping is disabled");
    validateSchema(newMetadata.getSchema(), true /*columnMappingEnabled*/);
    validatePartitionColumns(
        newMetadata.getSchema(), new ArrayList<>(newMetadata.getPartitionColNames()));
    int currentMaxFieldId =
        Integer.parseInt(
            currentMetadata.getConfiguration().getOrDefault(COLUMN_MAPPING_MAX_COLUMN_ID_KEY, "0"));
    return validateSchemaEvolution(
        currentMetadata.getSchema(),
        newMetadata.getSchema(),
        ColumnMapping.getColumnMappingMode(newMetadata.getConfiguration()),
        clusteringColumnPhysicalNames,
        currentMaxFieldId,
        allowNewRequiredFields,
        TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(newMetadata.getConfiguration()),
        TableConfig.TYPE_WIDENING_ENABLED.fromMetadata(newMetadata.getConfiguration()));
  }

  /**
   * Validates a given schema evolution by using field ID as the source of truth for identifying
   * fields
   *
   * @param currentSchema the schema that is present the table schema _before_ the schema evolution
   * @param newSchema the new schema that is present the table schema _after_ the schema evolution
   * @param clusteringColumnPhysicalNames The clustering columns present in the table before the
   *     schema update
   * @param oldMaxFieldId the maximum field id in the table before the schema update
   * @param allowNewRequiredFields If `false`, adding new required columns throws an error. If
   *     `true`, new required columns are allowed
   * @param icebergWriterCompatV1Enabled `true` if icebergCompatV1 is enabled on the table
   * @return an updated schema if metadata (e.g. TypeChanges needs to be copied over from the old
   *     schema
   * @throws IllegalArgumentException if the schema evolution is invalid
   */
  // TODO: Consider renaming or refactoring to avoid returning the
  // StructType here.
  public static Optional<StructType> validateSchemaEvolutionById(
      StructType currentSchema,
      StructType newSchema,
      Set<String> clusteringColumnPhysicalNames,
      int oldMaxFieldId,
      boolean allowNewRequiredFields,
      boolean icebergWriterCompatV1Enabled,
      boolean typeWideningEnabled) {
    SchemaChanges schemaChanges = computeSchemaChangesById(currentSchema, newSchema);
    validatePhysicalNameConsistency(schemaChanges.updatedFields());
    // Validates that the updated schema does not contain breaking changes in terms of types and
    // nullability
    validateUpdatedSchemaCompatibility(
        schemaChanges,
        oldMaxFieldId,
        allowNewRequiredFields,
        icebergWriterCompatV1Enabled,
        typeWideningEnabled);
    validateClusteringColumnsNotDropped(
        schemaChanges.removedFields(), clusteringColumnPhysicalNames);
    return schemaChanges.updatedSchema();
    // ToDo Potentially validate IcebergCompatV2 nested IDs
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
   * Verify the clustering columns exists in the table schema.
   *
   * @param schema The schema of the table
   * @param clusteringCols List of clustering columns
   */
  public static List<Column> casePreservingEligibleClusterColumns(
      StructType schema, List<Column> clusteringCols) {

    List<Tuple2<Column, DataType>> physicalColumnsWithTypes =
        clusteringCols.stream()
            .map(col -> ColumnMapping.getPhysicalColumnNameAndDataType(schema, col))
            .collect(Collectors.toList());

    List<String> nonSkippingEligibleColumns =
        physicalColumnsWithTypes.stream()
            .filter(tuple -> !StatsSchemaHelper.isSkippingEligibleDataType(tuple._2))
            .map(tuple -> tuple._1.toString() + " : " + tuple._2)
            .collect(Collectors.toList());

    if (!nonSkippingEligibleColumns.isEmpty()) {
      throw new KernelException(
          format(
              "Clustering is not supported because the following column(s): %s "
                  + "don't support data skipping",
              nonSkippingEligibleColumns));
    }

    return physicalColumnsWithTypes.stream().map(tuple -> tuple._1).collect(Collectors.toList());
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

  /** @return column name by concatenating the column path elements (think of nested) with dots */
  public static String concatWithDot(List<String> columnPath) {
    return columnPath.stream().map(SchemaUtils::escapeDots).collect(Collectors.joining("."));
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Private methods                                                                           ///
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /* Compute the SchemaChanges using field IDs */
  static SchemaChanges computeSchemaChangesById(StructType currentSchema, StructType newSchema) {
    SchemaChanges.Builder schemaDiff = SchemaChanges.builder();
    findAndAddRemovedFields(currentSchema, newSchema, schemaDiff);

    // Given a schema like struct<a (id=1) : map<int,struct<b (id=2) : Int >>
    // This map would contain:
    // {<"", 1> : StructField("a", MapType),
    // <"key", 1> : StructField("key", IntegerType),
    // <"value", 1> : StructField("value", StructType),
    // <"", 2>: StructField("b", IntegerType)}
    Map<SchemaElementId, StructField> currentFieldIdToField = fieldsByElementId(currentSchema);
    Set<Integer> addedFieldIds = new HashSet<>();
    SchemaIterable newSchemaIterable = new SchemaIterable(newSchema);
    Iterator<SchemaIterable.MutableSchemaElement> newSchemaIterator =
        newSchemaIterable.newMutableIterator();

    boolean newSchemaHasUpdates = false;
    while (newSchemaIterator.hasNext()) {
      SchemaIterable.MutableSchemaElement newElement = newSchemaIterator.next();
      SchemaElementId id = getSchemaElementId(newElement);
      if (addedFieldIds.contains(id.getId())) {
        // Skip early if this is a descendant of an added field.
        continue;
      }

      StructField existingField = currentFieldIdToField.get(id);
      if (existingField == null) {
        // If the field wasn't present in the schema before then it represents either
        // a schema change or a newly added field. To check if it is a new struct field,
        // we check the old schema for just the field ID (i.e. no nested path) to make
        // a determination between these two cases.
        // If new StructField where added to the schema example above (e.g
        // <a (id=1) : map<int,struct<b (id=2) : Int, c (id=3) : Int>>> )
        // This would eventually probe the currentFieldIdToField map for <"", 3> and
        // find it is an addition.
        SchemaElementId rootId = new SchemaElementId(/* nestedPath =*/ "", id.getId());
        if (!currentFieldIdToField.containsKey(rootId)) {
          addedFieldIds.add(id.getId());
          schemaDiff.withAddedField(newElement.getNearestStructFieldAncestor());
        }
        // Getting here implies a structural change in nested maps/arrays which will be caught at
        // when comparing the higher level element (or we added a field in the if statement above).
        // This can be somewhat subtle, if this is a type change.
        // Consider the case of the changing a field from Map to an Array. This would imply that
        // path would be "element" here and either "key" or "value" in the previous schema. Nothing
        // is done for the new "element" path if it isn't a field addition there must be at least
        // one ancestor node in common (at least the nearest struct field) which would get added as
        // an update below. This logic is inductive. If the previous schema was a array<array<x>>
        // and was not a new addition and the new schema was array<array<array<x>>> then this path
        // would be reached on element.element.element but the type the code would move past this
        // block for element.element which would have a type change detected from x to array<x>.
        // concretely if the new schema was <a id=1 : array<struct<b (id=2) : Int>>> then
        // <"element, "1"> would be skipped here but the type change would be detected for <"", 1>
        // from map to array.
        continue;
      }
      StructField newField = newElement.getField();
      List<TypeChange> existingTypeChanges = existingField.getTypeChanges();
      if (!existingTypeChanges.isEmpty() && newField.getTypeChanges().isEmpty()) {
        // Copy over type changes from existing field because new schemas
        // might be constructed elsewhere and not have the persisted type
        // change metadata.
        newField = newField.withTypeChanges(existingTypeChanges);
        newElement.updateField(newField);
        newSchemaHasUpdates = true;
      }

      // Ensure the schemas are equal now, updating type changes from clients is not supported
      // so they should be.
      if (!existingTypeChanges.equals(newField.getTypeChanges())) {
        throw new KernelException(
            String.format(
                "Detected a modified type changes field at '%s' %s != %s",
                newElement.getNamePath(), existingTypeChanges, newField.getTypeChanges()));
      }

      if (!existingField.equals(newField)) {
        if (!isNestedType(existingField)
            && !isNestedType(newField)
            && !existingField.getDataType().equivalent(newField.getDataType())) {
          // Type changes only apply to non-nested types.  This loop evaluates both
          // nested and non-nested, so we narrow down updates here. Actual type differences
          // between nested types are validated against SchemaChanges returned by this
          // function.
          List<TypeChange> changes = new ArrayList<>(newField.getTypeChanges());
          changes.add(new TypeChange(existingField.getDataType(), newField.getDataType()));
          newElement.updateField(newField.withTypeChanges(changes));
          newSchemaHasUpdates = true;
        }
        // Field changed name, nullability, metadata or type
        schemaDiff.withUpdatedField(existingField, newField, newElement.getNamePath());
      }
    }
    if (newSchemaHasUpdates) {
      schemaDiff.withUpdatedSchema(newSchemaIterable.getSchema());
    }
    return schemaDiff.build();
  }

  private static boolean isNestedType(StructField existingField) {
    // TODO: Make is nested a centralized concept
    DataType d = existingField.getDataType();
    return d instanceof StructType || d instanceof ArrayType || d instanceof MapType;
  }

  private static Map<SchemaElementId, StructField> fieldsByElementId(StructType schema) {
    Map<SchemaElementId, StructField> fieldIdToField = new HashMap<>();
    for (SchemaIterable.SchemaElement element : new SchemaIterable(schema)) {
      SchemaElementId id = getSchemaElementId(element);
      checkArgument(
          !fieldIdToField.containsKey(id),
          "Field %s with id %d already exists",
          element.getNamePath(),
          id);
      fieldIdToField.put(id, element.getField());
    }
    return fieldIdToField;
  }

  private static SchemaElementId getSchemaElementId(SchemaIterable.SchemaElement element) {
    int columnId = getColumnId(element.getNearestStructFieldAncestor());
    return new SchemaElementId(element.getPathFromNearestStructFieldAncestor(""), columnId);
  }

  private static void findAndAddRemovedFields(
      StructType currentSchema, StructType newSchema, SchemaChanges.Builder schemaDiff) {
    // With schema: <a (id=1) : map<int,struct<b (id=2) : Int, c (id=3) : Int>>>
    // contains {1: StructField("a", MapType), 3: StructField("c", IntegerType)}
    Map<Integer, StructField> fieldIdToField = fieldsById(newSchema);
    for (SchemaIterable.SchemaElement element : new SchemaIterable(currentSchema)) {
      // Removed fields are always calculated at the Struct level.
      // From the example above, we only "c" or "a" are removed, as all other StructFields
      // returned by the iterator (e.g. a.key, a.value cannot be removed without a type
      // change).
      if (!element.isStructField()) {
        continue;
      }
      StructField field = element.getField();
      int columnId = getCheckedColumnId(field);
      if (!fieldIdToField.containsKey(columnId)) {
        schemaDiff.withRemovedField(element.getField());
      }
    }
  }

  private static void validatePhysicalNameConsistency(
      List<SchemaChanges.SchemaUpdate> updatedFields) {
    for (SchemaChanges.SchemaUpdate updatedField : updatedFields) {
      StructField currentField = updatedField.before();
      StructField newField = updatedField.after();
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

  /*
   * Validate if a given schema evolution is safe for a given column mapping mode
   *
   * <p>Returns an updated schema if metadata (i.e. TypeChanges needs to be copied
   * over from currentSchema).
   */
  private static Optional<StructType> validateSchemaEvolution(
      StructType currentSchema,
      StructType newSchema,
      ColumnMappingMode columnMappingMode,
      Set<String> clusteringColumnPhysicalNames,
      int currentMaxFieldId,
      boolean allowNewRequiredFields,
      boolean icebergWriterCompatV1Enabled,
      boolean typeWideningEnabled) {
    switch (columnMappingMode) {
      case ID:
      case NAME:
        return validateSchemaEvolutionById(
            currentSchema,
            newSchema,
            clusteringColumnPhysicalNames,
            currentMaxFieldId,
            allowNewRequiredFields,
            icebergWriterCompatV1Enabled,
            typeWideningEnabled);
      case NONE:
        throw new UnsupportedOperationException(
            "Schema evolution without column mapping is not supported");
      default:
        throw new UnsupportedOperationException(
            "Unknown column mapping mode: " + columnMappingMode);
    }
  }

  private static void validateClusteringColumnsNotDropped(
      List<StructField> droppedFields, Set<String> clusteringColumnPhysicalNames) {
    for (StructField droppedField : droppedFields) {
      // ToDo: At some point plumb through mapping of ID to full name, so we get better error
      // messages
      if (clusteringColumnPhysicalNames.contains(getPhysicalName(droppedField))) {
        throw new KernelException(
            String.format("Cannot drop clustering column %s", droppedField.getName()));
      }
    }
  }

  /**
   * Verifies that no non-nullable fields are added, no existing field nullability is tightened and
   * no invalid type changes are performed
   *
   * <p>ToDo: Prevent moving fields outside of their containing struct
   */
  private static void validateUpdatedSchemaCompatibility(
      SchemaChanges schemaChanges,
      int oldMaxFieldId,
      boolean allowNewRequiredFields,
      boolean icebergWriterCompatV1Enabled,
      boolean typeWideningEnabled) {
    for (StructField addedField : schemaChanges.addedFields()) {
      if (!allowNewRequiredFields && !addedField.isNullable()) {
        throw new KernelException(
            String.format("Cannot add non-nullable field %s", addedField.getName()));
      }
      int colId = getColumnId(addedField);
      if (colId <= oldMaxFieldId) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot add a new column with a fieldId <= maxFieldId. Found field: %s with"
                    + "fieldId=%s. Current maxFieldId in the table is: %s",
                addedField, colId, oldMaxFieldId));
      }
    }

    for (SchemaChanges.SchemaUpdate updatedFields : schemaChanges.updatedFields()) {
      validateFieldCompatibility(updatedFields, icebergWriterCompatV1Enabled, typeWideningEnabled);
    }
  }

  /**
   * Validate that there was no change in type from existing field from new field, excluding
   * modified, dropped, or added fields to structs. Validates that a field's nullability is not
   * tightened
   */
  private static void validateFieldCompatibility(
      SchemaChanges.SchemaUpdate update,
      boolean icebergWriterCompatV1Enabled,
      boolean typeWideningEnabled) {
    StructField existingField = update.before();
    StructField newField = update.after();
    if (existingField.isNullable() && !newField.isNullable()) {
      throw new KernelException(
          String.format(
              "Cannot tighten the nullability of existing field %s", update.getPathToAfterField()));
    }

    if (existingField.getDataType() instanceof MapType
        && newField.getDataType() instanceof MapType) {
      MapType existingMapType = (MapType) existingField.getDataType();
      MapType newMapType = (MapType) newField.getDataType();

      if (icebergWriterCompatV1Enabled
          && existingMapType.getKeyType() instanceof StructType
          && newMapType.getKeyType() instanceof StructType) {
        // Enforce that we don't change map struct keys. This is a requirement for
        // IcebergWriterCompatV1
        StructType currentKeyType = (StructType) existingMapType.getKeyType();
        StructType newKeyType = (StructType) newMapType.getKeyType();
        if (!currentKeyType.equals(newKeyType)) {
          throw new KernelException(
              String.format(
                  "Cannot change the type key of Map field %s from %s to %s",
                  newField.getName(), currentKeyType, newKeyType));
        }
      }
    } else {
      // Note because computeSchemaChangesById() adds all changed struct fields and any nested
      // elements that have the same path but different types then the following scenarios are
      // handled in this block.
      // 1. This is non-leaf node (e.g. StructType, ArrayType) type, in which case it is sufficient
      // to ensure that the types are of the same class. For a struct type there might be field
      // additions or removals, which shouldn't be considered invalid schema transitions
      // (and hence `equivalent(...)` is not used in this case). If the nested type changes
      // (e.g. ArrayType to Primitive) then the classes would be different and the types by
      // definition would not be equivalent.
      //
      // 2. This is a leaf node, in which case it sufficient to check that the types are equivalent
      // (or once implemented the transition of types is valid).
      //
      // The subtle point here is for any non-leaf node change, the computed changes will include at
      // least one ancestor change where the type change can be detected.
      //
      for (Class<?> clazz : new Class<?>[] {StructType.class, ArrayType.class}) {
        if (existingField.getDataType().getClass() == clazz
            && newField.getDataType().getClass() == clazz) {
          return;
        }
      }
      DataType existingType = existingField.getDataType();
      DataType newType = newField.getDataType();
      if (!existingType.equivalent(newType)) {

        if (typeWideningEnabled) {
          if ((icebergWriterCompatV1Enabled
                  && TypeWideningChecker.isIcebergV2Compatible(existingType, newType))
              || (!icebergWriterCompatV1Enabled
                  && TypeWideningChecker.isWideningSupported(existingType, newType))) {
            return;
          }
        }
        throw new KernelException(
            String.format(
                "Cannot change the type of existing field %s from %s to %s",
                existingField.getName(), existingField.getDataType(), newField.getDataType()));
      }
    }
  }

  /**
   * A composite class that uniquely identifiers an element in a schema.
   *
   * <p>When column mapping is enabled, the every field in structs has a unique ID. For other nested
   * types (maps and arrays), elements are identified from there path from the struct field.
   */
  private static class SchemaElementId {
    private final String nestedPath;
    private final int id;

    SchemaElementId(String nestedPath, int id) {
      this.nestedPath = nestedPath;
      this.id = id;
    }

    public int getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SchemaElementId that = (SchemaElementId) o;
      return id == that.id && Objects.equals(nestedPath, that.nestedPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nestedPath, id);
    }

    @Override
    public String toString() {
      return "SchemaElementId{" + "getPath='" + nestedPath + '\'' + ", id=" + id + '}';
    }
  }

  private static Map<Integer, StructField> fieldsById(StructType schema) {
    Map<Integer, StructField> columnIdToField = new HashMap<>();
    for (SchemaIterable.SchemaElement element : new SchemaIterable(schema)) {
      if (!element.isStructField()) {
        continue;
      }
      StructField field = element.getField();
      int columnId = getCheckedColumnId(field);
      checkArgument(
          !columnIdToField.containsKey(columnId),
          "Field %s with id %d already exists",
          field.getName(),
          columnId);
      columnIdToField.put(columnId, field);
    }

    return columnIdToField;
  }

  private static int getCheckedColumnId(StructField field) {
    checkArgument(hasColumnId(field), "Field %s is missing column id", field.getName());
    checkArgument(hasPhysicalName(field), "Field %s is missing physical name", field.getName());
    return ColumnMapping.getColumnId(field);
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
