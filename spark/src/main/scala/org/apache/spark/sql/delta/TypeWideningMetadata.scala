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

import scala.collection.mutable

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.types._

/**
 * Information corresponding to a single type change.
 * @param version   (Deprecated) The version of the table where the type change was made. This is
 *                  only populated by clients using the preview of type widening.
 * @param fromType  The original type before the type change.
 * @param toType    The new type after the type change.
 * @param fieldPath The path inside nested maps and arrays to the field where the type change was
 *                  made. Each path element is either `key`/`value` for maps or `element` for
 *                  arrays. The path is empty if the type change was applied inside a map or array.
 */
private[delta] case class TypeChange(
    version: Option[Long],
    fromType: DataType,
    toType: DataType,
    fieldPath: Seq[String]) {
  import TypeChange._

  /** Serialize this type change to a [[Metadata]] object. */
  def toMetadata: Metadata = {
    val builder = new MetadataBuilder()
    version.foreach(builder.putLong(TABLE_VERSION_METADATA_KEY, _))
    builder
      .putString(FROM_TYPE_METADATA_KEY, fromType.typeName)
      .putString(TO_TYPE_METADATA_KEY, toType.typeName)
    if (fieldPath.nonEmpty) {
      builder.putString(FIELD_PATH_METADATA_KEY, fieldPath.mkString("."))
    }
    builder.build()
  }
}

private[delta] object TypeChange {
  // tableVersion was a field present during the preview and removed afterwards. We preserve it if
  // it's already present in the type change metadata of the table to avoid breaking older clients.
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
    val version = if (metadata.contains(TABLE_VERSION_METADATA_KEY)) {
      Some(metadata.getLong(TABLE_VERSION_METADATA_KEY))
    } else {
      None
    }
    TypeChange(
      version,
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

private[delta] object TypeWideningMetadata extends DeltaLogging {
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

    val changesToRecord = mutable.Buffer.empty[TypeChange]
    val schemaWithMetadata = SchemaMergingUtils.transformColumns(schema, oldSchema) {
      case (_, newField, Some(oldField), _) =>
        var typeChanges = collectTypeChanges(oldField.dataType, newField.dataType)
        // The version field isn't used anymore but we need to populate it in case the table doesn't
        // use the stable feature, as preview clients may then still access the table and rely on
        // the field being present.
        if (!txn.protocol.isFeatureSupported(TypeWideningTableFeature)) {
          typeChanges = typeChanges.map { change =>
            change.copy(version = Some(txn.getFirstAttemptVersion))
          }
        }

        changesToRecord ++= typeChanges
        TypeWideningMetadata(typeChanges).appendToField(newField)
      case (_, newField, None, _) =>
        // The field was just added, no need to process.
        newField
    }

    if (changesToRecord.nonEmpty) {
      recordDeltaEvent(
        deltaLog = txn.snapshot.deltaLog,
        opType = "delta.typeWidening.typeChanges",
        data = Map(
          "changes" -> changesToRecord.map { change =>
            Map(
              "fromType" -> change.fromType.sql,
              "toType" -> change.toType.sql)
          }
        ))
    }
    schemaWithMetadata
  }

  /**
   * Recursively collect primitive type changes inside nested maps and arrays between `fromType` and
   * `toType`.
   */
  private def collectTypeChanges(fromType: DataType, toType: DataType)
    : Seq[TypeChange] = (fromType, toType) match {
    case (from: MapType, to: MapType) =>
      collectTypeChanges(from.keyType, to.keyType).map { typeChange =>
        typeChange.copy(fieldPath = "key" +: typeChange.fieldPath)
      } ++
      collectTypeChanges(from.valueType, to.valueType).map { typeChange =>
        typeChange.copy(fieldPath = "value" +: typeChange.fieldPath)
      }
    case (from: ArrayType, to: ArrayType) =>
      collectTypeChanges(from.elementType, to.elementType).map { typeChange =>
        typeChange.copy(fieldPath = "element" +: typeChange.fieldPath)
      }
    case (fromType: AtomicType, toType: AtomicType) if fromType != toType =>
      Seq(TypeChange(
        version = None,
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
   *
   * Note: The `tableVersion` field is only populated for tables that use the preview of type
   * widening, we could remove this if/when there are no more tables using the preview of the
   * feature.
   */
  def updateTypeChangeVersion(schema: StructType, fromVersion: Long, toVersion: Long): StructType =
    SchemaMergingUtils.transformColumns(schema) {
      case (_, field, _) =>
        fromField(field) match {
          case Some(typeWideningMetadata) =>
            val updatedTypeChanges = typeWideningMetadata.typeChanges.map {
              case typeChange if typeChange.version.contains(fromVersion) =>
                typeChange.copy(version = Some(toVersion))
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

  /**
   * Remove the type widening metadata from all the fields in the given schema.
   * Return the cleaned schema and a list of fields with their path that had type widening metadata.
   */
  def removeTypeWideningMetadata(schema: StructType)
    : (StructType, Seq[(Seq[String], StructField)]) = {
    if (!containsTypeWideningMetadata(schema)) return (schema, Seq.empty)

    val changes = mutable.Buffer.empty[(Seq[String], StructField)]
    val newSchema = SchemaMergingUtils.transformColumns(schema) {
      case (fieldPath: Seq[String], field: StructField, _)
        if field.metadata.contains(TYPE_CHANGES_METADATA_KEY) =>
          changes.append((fieldPath, field))
          val cleanMetadata = new MetadataBuilder()
            .withMetadata(field.metadata)
            .remove(TYPE_CHANGES_METADATA_KEY)
            .build()
          field.copy(metadata = cleanMetadata)
      case (_, field: StructField, _) => field
    }
    newSchema -> changes.toSeq
  }

  /** Recursively checks whether any struct field in the schema contains type widening metadata. */
  def containsTypeWideningMetadata(schema: StructType): Boolean =
    schema.existsRecursively {
      case s: StructType => s.exists(_.metadata.contains(TYPE_CHANGES_METADATA_KEY))
      case _ => false
    }

  /**
   * Return all type changes recorded in the table schema.
   * @return A list of tuples (field path, type change).
   */
  def getAllTypeChanges(schema: StructType): Seq[(Seq[String], TypeChange)] = {
    if (!containsTypeWideningMetadata(schema)) return Seq.empty

    val allStructFields = SchemaUtils.filterRecursively(schema, checkComplexTypes = true) {
      _ => true
    }

    def getTypeChanges(field: StructField): Seq[TypeChange] =
      fromField(field)
        .map(_.typeChanges)
        .getOrElse(Seq.empty)

    allStructFields.flatMap { case (fieldPath, field) =>
      getTypeChanges(field).map((fieldPath :+ field.name, _))
    }
  }
}
