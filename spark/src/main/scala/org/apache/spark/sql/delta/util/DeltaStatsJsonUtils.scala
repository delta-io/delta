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

package org.apache.spark.sql.delta.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.spark.sql.delta.shims.VariantStatsShims
import org.apache.spark.sql.delta.util.Codec.Base85Codec
import org.apache.spark.types.variant.{Variant, VariantUtil}
import org.apache.spark.unsafe.types.VariantVal

// Serializer to help serialize VariantVal's as Z85 strings in JSON.
class VariantValJsonSerializer extends StdSerializer[VariantVal](classOf[VariantVal]) {
  override def serialize(
      v: VariantVal,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeString(DeltaStatsJsonUtils.encodeVariantAsZ85(
      new Variant(v.getValue, v.getMetadata)))
  }
}

// Deserializer to help deserialize VariantVal's from Z85 strings.
class VariantValJsonDeserializer extends StdDeserializer[VariantVal](classOf[VariantVal]) {
  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): VariantVal = {
    val z85String = p.getText
    DeltaStatsJsonUtils.decodeVariantFromZ85(z85String)
  }
}

/**
 * Utility functions for serializing Delta stats to JSON with custom Variant encoding.
 * This object provides JSON serialization that encodes VariantVal objects as Z85 strings,
 * which is used for storing variant statistics in Delta checkpoints and stats.
 */
object DeltaStatsJsonUtils {

  /**
   * Jackson mapper configured to serialize/deserialize VariantVal as Z85-encoded strings.
   * The module is registered once to avoid memory leaks.
   */
  private val mapper = {
    val m = new ObjectMapper with ScalaObjectMapper
    m.setSerializationInclusion(Include.NON_ABSENT)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.registerModule(DefaultScalaModule)

    val variantModule = new SimpleModule()
    variantModule.addSerializer(classOf[VariantVal], new VariantValJsonSerializer())
    variantModule.addDeserializer(classOf[VariantVal], new VariantValJsonDeserializer())
    m.registerModule(variantModule)

    m
  }

  /**
   * Serialize an object to JSON, encoding any VariantVal fields as Z85 strings.
   * Same interface as JsonUtils.toJson but with custom Variant serialization.
   */
  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  /**
   * Deserialize JSON to an object, decoding any Z85 strings to VariantVal.
   * Same interface as JsonUtils.fromJson but with custom Variant deserialization.
   */
  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json)
  }

  /**
   * Encode a Variant as a Z85 string.
   * The variant binary format stores metadata followed by value bytes.
   * This concatenates them and encodes as Z85.
   */
  def encodeVariantAsZ85(v: Variant): String = {
    val metadata = v.getMetadata
    val value = v.getValue

    val combined = new Array[Byte](metadata.length + value.length)
    System.arraycopy(metadata, 0, combined, 0, metadata.length)
    System.arraycopy(value, 0, combined, metadata.length, value.length)

    Base85Codec.encodeBytes(combined)
  }

  /**
   * Decode a Z85-encoded string back to a VariantVal.
   */
  def decodeVariantFromZ85(z85: String): VariantVal = {
    val decoded = Base85Codec.decodeBytes(z85, z85.length)
    val metadataSize = VariantStatsShims.metadataSize(decoded)
    val valueWithPadding = decoded.slice(metadataSize, decoded.length)
    val valueSize = VariantUtil.valueSize(valueWithPadding, 0)
    val value = valueWithPadding.slice(0, valueSize)
    val metadata = decoded.slice(0, metadataSize)
    new VariantVal(value, metadata)
  }
}
