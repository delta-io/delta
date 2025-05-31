/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * SchemaChanges encapsulates a list of added, removed, renamed, or updated fields in a schema
 * change. Updates include renamed fields, reordered fields, type changes, nullability changes, and
 * metadata attribute changes. This set of updates can apply to nested fields within structs. In
 * case any update is applied to a nested field, an update will be produced for every level of
 * nesting. This includes re-ordered columns in a nested field. Note that SchemaChanges does not
 * capture re-ordered columns in top level schema.
 *
 * <p>For example, given a field struct_col: struct<inner_struct<id: int>> if id is renamed to
 * `renamed_id` 1 update will be produced for the change to struct_col and 1 update will be produced
 * for the change to inner_struct
 *
 * <p>ToDo: Possibly track moves/renames independently, enable capturing re-ordered columns in top
 * level schema
 */
class SchemaChanges {
  public static class SchemaUpdate {
    private final StructField fieldBefore;
    private final StructField fieldAfter;
    // This is a "." concatenated path to the field. Names containing "." are wrapped in
    // back-ticks (`).
    // For example in the schema <a.b : array<StructType<c : Int>>> the path to "c" would be:
    // "`a.b`.element.c". In general, though the format should not be relid upon since this
    // is used for surfacing errors to users.
    // Note this is a by name. If we want to be able to track changes
    // at the where an element is moved to a different location in the
    // schema we need to add more paths here.
    private final String pathToAfterField;

    SchemaUpdate(StructField fieldBefore, StructField fieldAfter, String pathToAfterField) {
      this.fieldBefore = fieldBefore;
      this.fieldAfter = fieldAfter;
      this.pathToAfterField = pathToAfterField;
    }

    public StructField before() {
      return fieldBefore;
    }

    public StructField after() {
      return fieldAfter;
    }

    public String getPathToAfterField() {
      return pathToAfterField;
    }
  }

  private List<StructField> addedFields;
  private List<StructField> removedFields;
  private List<SchemaUpdate> updatedFields;
  private Optional<StructType> updatedSchema;

  private SchemaChanges(
      List<StructField> addedFields,
      List<StructField> removedFields,
      List<SchemaUpdate> updatedFields,
      Optional<StructType> updatedSchema) {
    this.addedFields = Collections.unmodifiableList(addedFields);
    this.removedFields = Collections.unmodifiableList(removedFields);
    this.updatedFields = Collections.unmodifiableList(updatedFields);
    this.updatedSchema = updatedSchema;
  }

  static class Builder {
    private List<StructField> addedFields = new ArrayList<>();
    private List<StructField> removedFields = new ArrayList<>();
    private List<SchemaUpdate> updatedFields = new ArrayList<>();
    private Optional<StructType> updatedSchema = Optional.empty();

    public Builder withAddedField(StructField addedField) {
      addedFields.add(addedField);
      return this;
    }

    public Builder withRemovedField(StructField removedField) {
      removedFields.add(removedField);
      return this;
    }

    public Builder withUpdatedField(
        StructField existingField, StructField newField, String pathToAfterField) {
      updatedFields.add(new SchemaUpdate(existingField, newField, pathToAfterField));
      return this;
    }

    public Builder withUpdatedSchema(StructType updatedSchema) {
      this.updatedSchema = Optional.of(updatedSchema);
      return this;
    }

    public SchemaChanges build() {
      return new SchemaChanges(addedFields, removedFields, updatedFields, updatedSchema);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /* Added Fields */
  public List<StructField> addedFields() {
    return addedFields;
  }

  /* Removed Fields */
  public List<StructField> removedFields() {
    return removedFields;
  }

  /* Updated Fields (e.g. rename, type change) represented */
  public List<SchemaUpdate> updatedFields() {
    return updatedFields;
  }

  public Optional<StructType> updatedSchema() {
    return updatedSchema;
  }
}
