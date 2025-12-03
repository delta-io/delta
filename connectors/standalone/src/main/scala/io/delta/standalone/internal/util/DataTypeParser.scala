/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import io.delta.standalone.types._

private[standalone] object DataTypeParser {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  private val nonDecimalNameToType = {
    Seq(new NullType, new DateType, new TimestampType, new BinaryType, new IntegerType,
      new BooleanType, new LongType, new DoubleType, new FloatType, new ShortType, new ByteType,
      new StringType).map(t => t.getTypeName -> t).toMap
  }

  def fromJson(json: String): DataType = parseDataType(parse(json))

  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      nameToType(name)

    case JSortedObject(
    ("containsNull", JBool(n)),
    ("elementType", t: JValue),
    ("type", JString("array"))) =>
      new ArrayType(parseDataType(t), n)

    case JSortedObject(
    ("keyType", k: JValue),
    ("type", JString("map")),
    ("valueContainsNull", JBool(n)),
    ("valueType", v: JValue)) =>
      new MapType(parseDataType(k), parseDataType(v), n)

    case JSortedObject(
    ("fields", JArray(fields)),
    ("type", JString("struct"))) =>
      new StructType(fields.map(parseStructField).toArray)

    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a data type.")
  }

  def toJson(value: DataType): String = compact(render(dataTypeToJValue(value)))

  def toPrettyJson(value: DataType): String = pretty(render(dataTypeToJValue(value)))

  private def dataTypeToJValue(dataType: DataType): JValue = dataType match {
    case array: ArrayType =>
      ("type" -> "array") ~
        ("elementType" -> dataTypeToJValue(array.getElementType)) ~
        ("containsNull" -> array.containsNull())
    case map: MapType =>
      ("type" -> "map") ~
        ("keyType" -> dataTypeToJValue(map.getKeyType())) ~
        ("valueType" -> dataTypeToJValue(map.getValueType())) ~
        ("valueContainsNull" -> map.valueContainsNull())
    case struct: StructType =>
      ("type" -> "struct") ~
        ("fields" -> struct.getFields().map(structFieldToJValue).toList)
    case decimal: DecimalType =>
      s"decimal(${decimal.getPrecision()},${decimal.getScale()})"
    case _: DataType =>
      dataType.getTypeName()
  }

  private def structFieldToJValue(field: StructField): JValue = {
    val name = field.getName()
    val dataType = field.getDataType()
    val nullable = field.isNullable()
    val metadata = field.getMetadata()

    ("name" -> name) ~
      ("type" -> dataTypeToJValue(dataType)) ~
      ("nullable" -> nullable) ~
      ("metadata" -> metadataValueToJValue(metadata))
  }

  private def metadataValueToJValue(value: Any): JValue = {
    value match {
      case metadata: FieldMetadata =>
        JObject(metadata.getEntries().entrySet().asScala.map(e =>
          (e.getKey(), metadataValueToJValue(e.getValue()))).toList)
      case arr: Array[Object] =>
        JArray(arr.toList.map(metadataValueToJValue))
      case x: Long =>
        JInt(x)
      case x: Double =>
        JDouble(x)
      case x: Boolean =>
        JBool(x)
      case x: String =>
        JString(x)
      case null =>
        JNull
      case other =>
        throw new IllegalArgumentException(
          s"Failed to convert ${value.getClass()} instance to JValue.")
    }
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => new DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType.getOrElse(
        other,
        throw new IllegalArgumentException(
          s"Failed to convert the JSON string '$name' to a data type."))
    }
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
    ("metadata", metadata: JObject),
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      new StructField(name, parseDataType(dataType), nullable, parseFieldMetadata(metadata))
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      new StructField(name, parseDataType(dataType), nullable)
    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a field.")
  }

  private def parseFieldMetadata(metadata: JObject): FieldMetadata = {
    val builder = FieldMetadata.builder()
    metadata.obj.foreach {
      case (key, JInt(value)) =>
        builder.putLong(key, value.toLong)
      case(key, JDouble(value)) =>
        builder.putDouble(key, value)
      case (key, JBool(value)) =>
        builder.putBoolean(key, value)
      case (key, JString(value)) =>
        builder.putString(key, value)
      case (key, o: JObject) =>
        builder.putMetadata(key, parseFieldMetadata(o))
      case (key, JArray(value)) =>
        if (value.isEmpty) {
          // If it is an empty array, we cannot infer its element type. We put an empty Array[Long].
          builder.putLongArray(key, Array.empty)
        } else {
          value.head match {
            case _: JInt =>
              builder.putLongArray(key,
                value.map(_.asInstanceOf[JInt].num.toLong.asInstanceOf[java.lang.Long]).toArray)
            case _: JDouble =>
              builder.putDoubleArray(key,
                value.asInstanceOf[List[JDouble]].map(_.num.asInstanceOf[java.lang.Double]).toArray)
            case _: JBool =>
              builder.putBooleanArray(key,
                value.asInstanceOf[List[JBool]].map(_.value.asInstanceOf[java.lang.Boolean])
                  .toArray)
            case _: JString =>
              builder.putStringArray(key, value.asInstanceOf[List[JString]].map(_.s).toArray)
            case _: JObject =>
              builder.putMetadataArray(key,
                value.asInstanceOf[List[JObject]].map(parseFieldMetadata).toArray)
            case other =>
              throw new IllegalArgumentException(
                s"Unsupported ${value.head.getClass()} Array as metadata value.")
          }
        }
      case (key, JNull) =>
        builder.putNull(key)
      case (key, other) =>
        throw new IllegalArgumentException(
          s"Unsupported ${other.getClass()} instance as metadata value.")
    }
    builder.build()
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.sortBy(_._1))
      case _ => None
    }
  }
}
