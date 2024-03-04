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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.types._

/**
 * Information corresponding to a single type change.
 * @param version   The version of the table where the type change was made.
 * @param fromType  The original type before the type change.
 * @param toType    The new type after the type change.
 * @param fieldPath The path inside nested maps and arrays to the field where the type change was
 *                  made. Each path element is either `key`/`value` for maps or `element` for
 *                  arrays. The path is empty if the type change was applied inside a map or array.
 */
private[delta] case class TypeChange(
    version: Long,
    fromType: DataType,
    toType: DataType,
    fieldPath: Seq[String]) {
  import TypeChange._

  /** Serialize this type change to a [[Metadata]] object. */
  def toMetadata: Metadata = {
    val builder = new MetadataBuilder()
      .putLong(TABLE_VERSION_METADATA_KEY, version)
      .putString(FROM_TYPE_METADATA_KEY, fromType.typeName)
      .putString(TO_TYPE_METADATA_KEY, toType.typeName)
    if (fieldPath.nonEmpty) {
      builder.putString(FIELD_PATH_METADATA_KEY, fieldPath.mkString("."))
    }
    builder.build()
  }
}

private[delta] object TypeChange {
  val TABLE_VERSION_METADATA_KEY: String = "tableVersion"
  val FROM_TYPE_METADATA_KEY: String = "fromType"
  val TO_TYPE_METADATA_KEY: String = "toType"
  val FIELD_PATH_METADATA_KEY: String = "fieldPath"

   /** Deserialize this type change from a [[Metadata]] object. */
  def fromMetadata(metadata: Metadata): TypeChange = {
    val fieldPath = if (metadata.contains(FIELD_PATH_METADATA_KEY)) {
      metadata.getString(FIELD_PATH_METADATA_KEY).split("\\.").toSeq
    } else {
      Seq.empty
    }
    TypeChange(
      version = metadata.getLong(TABLE_VERSION_METADATA_KEY),
      fromType = DataType.fromDDL(metadata.getString(FROM_TYPE_METADATA_KEY)),
      toType = DataType.fromDDL(metadata.getString(TO_TYPE_METADATA_KEY)),
      fieldPath
    )
  }
}

/**
 * Represents all type change information for a single struct field
 * @param typeChanges The type changes that have been applied to the field.
 */
private[delta] case class TypeWideningMetadata(typeChanges: Seq[TypeChange]) {

  import TypeWideningMetadata._

  /**
   * Add the type changes to the metadata of the given field, preserving any pre-existing type
   * widening metadata.
   */
  def appendToField(field: StructField): StructField = {
    if (typeChanges.isEmpty) return field

    val existingTypeChanges = fromField(field).map(_.typeChanges).getOrElse(Seq.empty)
    val allTypeChanges = existingTypeChanges ++ typeChanges

    val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
      .putMetadataArray(TYPE_CHANGES_METADATA_KEY, allTypeChanges.map(_.toMetadata).toArray)
      .build()
    field.copy(metadata = newMetadata)
  }
}

private[delta] object TypeWideningMetadata {
  val TYPE_CHANGES_METADATA_KEY: String = "delta.typeChanges"

  /** Read the type widening metadata from the given field. */
  def fromField(field: StructField): Option[TypeWideningMetadata] = {
    Option.when(field.metadata.contains(TYPE_CHANGES_METADATA_KEY)) {
      val typeChanges = field.metadata.getMetadataArray(TYPE_CHANGES_METADATA_KEY)
        .map { changeMetadata =>
          TypeChange.fromMetadata(changeMetadata)
        }.toSeq
      TypeWideningMetadata(typeChanges)
    }
  }

  /**
   * Computes the type changes from `oldSchema` to `schema` and adds corresponding type change
   * metadata to `schema`.
   */
  def addTypeWideningMetadata(
      txn: OptimisticTransaction,
      schema: StructType,
      oldSchema: StructType): StructType = {

    if (!TypeWidening.isEnabled(txn.protocol, txn.metadata)) return schema

    if (DataType.equalsIgnoreNullability(schema, oldSchema)) return schema

    SchemaMergingUtils.transformColumns(schema, oldSchema) {
      case (_, newField, Some(oldField), _) =>
        // Record the version the transaction will attempt to use in the type change metadata. If
        // there's a conflict with another transaction, the version in the metadata will be updated
        // during conflict resolution. See [[ConflictChecker.updateTypeWideningMetadata()]].
        val typeChanges =
          collectTypeChanges(oldField.dataType, newField.dataType, txn.getFirstAttemptVersion)
        TypeWideningMetadata(typeChanges).appendToField(newField)
      case (_, newField, None, _) =>
        // The field was just added, no need to process.
        newField
    }
  }

  /**
   * Recursively collect primitive type changes inside nested maps and arrays between `fromType` and
   * `toType`. The `version` is the version of the table where the type change was made.
   */
  private def collectTypeChanges(fromType: DataType, toType: DataType, version: Long)
    : Seq[TypeChange] = (fromType, toType) match {
    case (from: MapType, to: MapType) =>
      collectTypeChanges(from.keyType, to.keyType, version).map { typeChange =>
        typeChange.copy(fieldPath = "key" +: typeChange.fieldPath)
      } ++
      collectTypeChanges(from.valueType, to.valueType, version).map { typeChange =>
        typeChange.copy(fieldPath = "value" +: typeChange.fieldPath)
      }
    case (from: ArrayType, to: ArrayType) =>
      collectTypeChanges(from.elementType, to.elementType, version).map { typeChange =>
        typeChange.copy(fieldPath = "element" +: typeChange.fieldPath)
      }
    case (fromType: AtomicType, toType: AtomicType) if fromType != toType =>
        Seq(TypeChange(
          version,
          fromType,
          toType,
          fieldPath = Seq.empty
        ))
    case (_: AtomicType, _: AtomicType) => Seq.empty
    // Don't recurse inside structs, `collectTypeChanges` should be called directly on each struct
    // fields instead to only collect type changes inside these fields.
    case (_: StructType, _: StructType) => Seq.empty
  }

  /**
   * Change the `tableVersion` value in the type change metadata present in `schema`. Used during
   * conflict resolution to update the version associated with the transaction is incremented.
   */
  def updateTypeChangeVersion(schema: StructType, fromVersion: Long, toVersion: Long): StructType =
    SchemaMergingUtils.transformColumns(schema) {
      case (_, field, _) =>
        fromField(field) match {
          case Some(typeWideningMetadata) =>
            val updatedTypeChanges = typeWideningMetadata.typeChanges.map {
              case typeChange if typeChange.version == fromVersion =>
                typeChange.copy(version = toVersion)
              case olderTypeChange => olderTypeChange
            }
            val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
              .putMetadataArray(
                TYPE_CHANGES_METADATA_KEY,
                updatedTypeChanges.map(_.toMetadata).toArray)
              .build()
            field.copy(metadata = newMetadata)

          case None => field
        }
    }
}
