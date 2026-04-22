/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.uccatalog

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  ArrayType => DrcArrayType,
  DecimalType => DrcDecimalType,
  DeltaType => DrcDeltaType,
  MapType => DrcMapType,
  PrimitiveType => DrcPrimitiveType,
  StructField => DrcStructField,
  StructType => DrcStructType
}

import org.apache.spark.sql.types._

/**
 * Converts a Delta REST Catalog (DRC) column list -- a `java.util.List` of UC SDK POJOs
 * -- into a Spark `StructType` without a JSON round-trip. The read-path fast path from
 * `DRCMetadataAdapter.getDRCColumns()` directly into `StructType`.
 *
 * Forward direction (DRC -> Spark) only. The reverse (Spark -> DRC) lands in the
 * write-path PR where intent-based schema emission matters.
 *
 * <p>Unknown primitive names or unrecognised `DeltaType` subclasses throw
 * `IllegalArgumentException`. Callers must not catch-and-coerce: an unknown type means
 * the UC server has rolled forward past the pinned SDK, and the Delta-side pin needs to
 * be bumped.
 */
private[delta] object DeltaRestSchemaConverter {

  def toSparkSchema(drcColumns: java.util.List[DrcStructField]): StructType = {
    require(drcColumns != null, "DRC column list must be non-null")
    StructType(drcColumns.asScala.map(toSparkField).toArray)
  }

  private def toSparkField(drc: DrcStructField): StructField = {
    require(drc.getName != null, "DRC column name must be non-null")
    require(drc.getType != null, s"DRC column '${drc.getName}' has null type")
    require(drc.getNullable != null, s"DRC column '${drc.getName}' has null 'nullable'")
    StructField(
      drc.getName,
      toSparkDataType(drc.getType),
      drc.getNullable,
      toSparkMetadata(drc.getMetadata))
  }

  private def toSparkDataType(drc: DrcDeltaType): DataType = drc match {
    case p: DrcPrimitiveType =>
      parsePrimitive(p.getType)
    case d: DrcDecimalType =>
      require(d.getPrecision != null, "decimal type missing precision")
      require(d.getScale != null, "decimal type missing scale")
      DecimalType(d.getPrecision, d.getScale)
    case a: DrcArrayType =>
      require(a.getElementType != null, "array type missing element-type")
      require(a.getContainsNull != null, "array type missing contains-null")
      ArrayType(toSparkDataType(a.getElementType), a.getContainsNull)
    case m: DrcMapType =>
      require(m.getKeyType != null, "map type missing key-type")
      require(m.getValueType != null, "map type missing value-type")
      require(m.getValueContainsNull != null, "map type missing value-contains-null")
      MapType(
        toSparkDataType(m.getKeyType),
        toSparkDataType(m.getValueType),
        m.getValueContainsNull)
    case s: DrcStructType =>
      require(s.getFields != null, "struct type missing fields")
      StructType(s.getFields.asScala.map(toSparkField).toArray)
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported DRC DeltaType subclass: ${other.getClass.getName}. " +
          "The pinned UC SDK may be ahead of the Delta-Spark converter -- bump the Delta side.")
  }

  private def parsePrimitive(name: String): DataType = name match {
    case "string" => StringType
    case "long" => LongType
    case "integer" => IntegerType
    case "short" => ShortType
    case "byte" => ByteType
    case "double" => DoubleType
    case "float" => FloatType
    case "boolean" => BooleanType
    case "binary" => BinaryType
    case "date" => DateType
    case "timestamp" => TimestampType
    case "timestamp_ntz" => TimestampNTZType
    case null =>
      throw new IllegalArgumentException("DRC primitive type name is null")
    case other =>
      throw new IllegalArgumentException(
        s"Unknown DRC primitive type name: '$other'. Add a case to " +
          "DeltaRestSchemaConverter.parsePrimitive or bump the UC pin if this is a new type.")
  }

  private def toSparkMetadata(raw: java.util.Map[String, Object]): Metadata = {
    if (raw == null || raw.isEmpty) return Metadata.empty
    val b = new MetadataBuilder
    raw.asScala.foreach { case (k, v) =>
      v match {
        case null => b.putNull(k)
        case s: String => b.putString(k, s)
        case l: java.lang.Long => b.putLong(k, l)
        case i: java.lang.Integer => b.putLong(k, i.longValue)
        case sh: java.lang.Short => b.putLong(k, sh.longValue)
        case by: java.lang.Byte => b.putLong(k, by.longValue)
        case bl: java.lang.Boolean => b.putBoolean(k, bl)
        case d: java.lang.Double => b.putDouble(k, d)
        case f: java.lang.Float => b.putDouble(k, f.doubleValue)
        case other => b.putString(k, String.valueOf(other))
      }
    }
    b.build()
  }
}
