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
  ArrayType => UCArrayType,
  DecimalType => UCDecimalType,
  DeltaType => UCDeltaType,
  MapType => UCMapType,
  PrimitiveType => UCPrimitiveType,
  StructField => UCStructField,
  StructType => UCStructType
}

import org.apache.spark.sql.types._

/**
 * Bidirectional converter between the Delta REST Catalog (DRC) schema POJO hierarchy and Spark's
 * `StructType`. The DRC wire format uses kebab-case (`element-type`, `contains-null`,
 * `value-contains-null`); Delta uses camelCase (`elementType`, `containsNull`). The UC SDK POJO
 * hierarchy handles the kebab/camel translation at (de)serialize time; this converter then walks
 * the POJO tree directly, skipping a JSON round-trip and preserving all field metadata
 * (`delta.columnMapping.id`, `delta.generationExpression`, CHECK-constraint expressions, column
 * defaults) verbatim.
 *
 * Used on the read path (`toSparkSchema`, consumed by `DeltaRestTableLoader`) and on the write
 * path (`toDRCColumns`, consumed by the DRC commit's `set-columns` update in a follow-up PR).
 */
private[delta] object DeltaRestSchemaConverter {

  // --------------------------------------------------------------------------
  // Read path: UC POJO columns -> Spark StructType
  // --------------------------------------------------------------------------

  /** Convert a DRC column list to a Spark `StructType`. Null input produces an empty schema. */
  def toSparkSchema(columns: java.util.List[UCStructField]): StructType = {
    if (columns == null) return new StructType()
    StructType(columns.asScala.iterator.map(toSparkField).toArray)
  }

  private[uccatalog] def toSparkField(col: UCStructField): StructField = {
    require(col != null, "column must not be null")
    require(col.getName != null, "column name must not be null")
    require(col.getType != null, s"column type must not be null for ${col.getName}")
    val dataType = toSparkDataType(col.getType)
    val nullable = Option(col.getNullable).map(_.booleanValue()).getOrElse(true)
    val metadata = toSparkMetadata(col.getMetadata)
    StructField(col.getName, dataType, nullable, metadata)
  }

  private def toSparkDataType(t: UCDeltaType): DataType = t match {
    case p: UCPrimitiveType => primitiveFromString(p.getType)
    case d: UCDecimalType =>
      val precision = Option(d.getPrecision).map(_.intValue()).getOrElse(
        throw new IllegalArgumentException("DecimalType missing precision"))
      val scale = Option(d.getScale).map(_.intValue()).getOrElse(
        throw new IllegalArgumentException("DecimalType missing scale"))
      DecimalType(precision, scale)
    case a: UCArrayType =>
      val element = toSparkDataType(a.getElementType)
      val containsNull = Option(a.getContainsNull).map(_.booleanValue()).getOrElse(true)
      ArrayType(element, containsNull)
    case m: UCMapType =>
      val keyType = toSparkDataType(m.getKeyType)
      val valueType = toSparkDataType(m.getValueType)
      val valueContainsNull =
        Option(m.getValueContainsNull).map(_.booleanValue()).getOrElse(true)
      MapType(keyType, valueType, valueContainsNull)
    case s: UCStructType =>
      val fields = s.getFields.asScala.iterator.map(toSparkField).toArray
      StructType(fields)
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported DRC DeltaType subclass: ${other.getClass.getName}")
  }

  private def primitiveFromString(name: String): DataType = name match {
    case null | "" =>
      throw new IllegalArgumentException("Primitive type identifier must not be null or empty")
    case "boolean" => BooleanType
    case "tinyint" | "byte" => ByteType
    case "smallint" | "short" => ShortType
    case "int" | "integer" => IntegerType
    case "long" | "bigint" => LongType
    case "float" => FloatType
    case "double" => DoubleType
    case "string" => StringType
    case "binary" => BinaryType
    case "date" => DateType
    case "timestamp" => TimestampType
    case "timestamp_ntz" => TimestampNTZType
    case "variant" => VariantType
    case s if s.startsWith("decimal(") && s.endsWith(")") =>
      // Defensive fallback when the wire sends a bare "decimal(p,s)" string instead of
      // a structured DecimalType object. The UC server should prefer the structured form.
      val inner = s.substring("decimal(".length, s.length - 1).split(",").map(_.trim)
      require(inner.length == 2, s"Bad decimal form: $s")
      DecimalType(inner(0).toInt, inner(1).toInt)
    case other =>
      throw new IllegalArgumentException(s"Unknown DRC primitive type: '$other'")
  }

  /**
   * Passes every column-metadata key through to Spark's `Metadata` structure verbatim.
   * Delta-specific metadata keys -- `delta.columnMapping.id`, `delta.generationExpression`,
   * `delta.identity.*`, CHECK-constraint expressions, column defaults -- must round-trip
   * exactly or downstream planner code (data skipping, column mapping, generated columns)
   * silently produces wrong results.
   */
  private def toSparkMetadata(m: java.util.Map[String, Object]): Metadata = {
    if (m == null || m.isEmpty) return Metadata.empty
    val builder = new MetadataBuilder
    m.asScala.foreach { case (k, v) => putMetadataValue(builder, k, v) }
    builder.build()
  }

  private def putMetadataValue(
      builder: MetadataBuilder, key: String, value: Object): Unit = value match {
    case null => builder.putNull(key)
    case s: String => builder.putString(key, s)
    case b: java.lang.Boolean => builder.putBoolean(key, b.booleanValue())
    case l: java.lang.Long => builder.putLong(key, l.longValue())
    case i: java.lang.Integer => builder.putLong(key, i.longValue())
    case d: java.lang.Double => builder.putDouble(key, d.doubleValue())
    case f: java.lang.Float => builder.putDouble(key, f.doubleValue())
    case other =>
      // Jackson may hand back a nested LinkedHashMap or ArrayList for non-scalar metadata.
      // Fall back to a stringified form rather than drop it silently -- losing metadata is
      // a silent-corruption risk (see `delta.generationExpression`, column-mapping ids, ...).
      builder.putString(key, other.toString)
  }

  // --------------------------------------------------------------------------
  // Write path: Spark StructType -> UC POJO columns
  // --------------------------------------------------------------------------

  /**
   * Convert a Spark `StructType` to the DRC column list used by `set-columns` in the commit
   * path. The resulting `List[UCStructField]` wraps each column's data type in the appropriate
   * `DeltaType` subclass (`PrimitiveType`, `DecimalType`, `ArrayType`, `MapType`, `StructType`)
   * with the discriminator `type` field set so Jackson can serialize directly.
   */
  def toDRCColumns(schema: StructType): java.util.List[UCStructField] = {
    if (schema == null) return java.util.Collections.emptyList()
    val out = new java.util.ArrayList[UCStructField](schema.fields.length)
    schema.fields.foreach(f => out.add(toDRCField(f)))
    out
  }

  private[uccatalog] def toDRCField(field: StructField): UCStructField = {
    require(field != null, "field must not be null")
    val out = new UCStructField().name(field.name).nullable(field.nullable)
    out.setType(toDRCDeltaType(field.dataType))
    writeMetadata(out, field.metadata)
    out
  }

  private def toDRCDeltaType(dt: DataType): UCDeltaType = dt match {
    case BooleanType => primitive("boolean")
    case ByteType => primitive("tinyint")
    case ShortType => primitive("smallint")
    case IntegerType => primitive("int")
    case LongType => primitive("long")
    case FloatType => primitive("float")
    case DoubleType => primitive("double")
    case StringType => primitive("string")
    case BinaryType => primitive("binary")
    case DateType => primitive("date")
    case TimestampType => primitive("timestamp")
    case TimestampNTZType => primitive("timestamp_ntz")
    case VariantType => primitive("variant")
    case DecimalType.Fixed(precision, scale) =>
      val dec = new UCDecimalType().precision(precision).scale(scale)
      dec.setType("decimal")
      dec
    case ArrayType(element, containsNull) =>
      val arr = new UCArrayType()
        .elementType(toDRCDeltaType(element))
        .containsNull(containsNull)
      arr.setType("array")
      arr
    case MapType(keyType, valueType, valueContainsNull) =>
      val m = new UCMapType()
        .keyType(toDRCDeltaType(keyType))
        .valueType(toDRCDeltaType(valueType))
        .valueContainsNull(valueContainsNull)
      m.setType("map")
      m
    case st: StructType =>
      val s = new UCStructType()
      val fields = new java.util.ArrayList[UCStructField](st.fields.length)
      st.fields.foreach(f => fields.add(toDRCField(f)))
      s.setFields(fields)
      s.setType("struct")
      s
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported Spark DataType for DRC: ${other.getClass.getName} ($other)")
  }

  private def primitive(name: String): UCPrimitiveType = {
    val p = new UCPrimitiveType()
    p.setType(name)
    p
  }

  /**
   * Serializes a Spark `Metadata` blob into UC's `Map<String, Object>` representation. Spark
   * metadata values are scalars (string, long, double, boolean, null), nested `Metadata`
   * blobs, or arrays of those; DRC's wire format accepts arbitrary JSON values. We preserve
   * scalars as-is; nested metadata and arrays are serialized to their JSON string form to keep
   * the write path lossless for keys Delta cares about.
   */
  private def writeMetadata(out: UCStructField, md: Metadata): Unit = {
    if (md == null || md == Metadata.empty) return
    // Spark's Metadata exposes a private map; json() is the stable public surface.
    val json = md.json
    // Parse back into (String, Object) pairs -- we reuse Jackson via a minimal parser.
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val tree = mapper.readTree(json)
    if (!tree.isObject) return
    val it = tree.fields()
    while (it.hasNext) {
      val entry = it.next()
      val v: AnyRef = jsonNodeToObject(entry.getValue)
      out.putMetadataItem(entry.getKey, v)
    }
  }

  private def jsonNodeToObject(n: com.fasterxml.jackson.databind.JsonNode): AnyRef = {
    if (n == null || n.isNull) null
    else if (n.isTextual) n.asText()
    else if (n.isBoolean) java.lang.Boolean.valueOf(n.asBoolean())
    else if (n.isIntegralNumber) java.lang.Long.valueOf(n.asLong())
    else if (n.isFloatingPointNumber) java.lang.Double.valueOf(n.asDouble())
    else n.toString // arrays / nested objects keep their JSON form
  }
}
