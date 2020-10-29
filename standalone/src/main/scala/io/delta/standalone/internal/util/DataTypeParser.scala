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
 * Copyright (2020) The Delta Lake Project Authors.
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

import io.delta.standalone.types._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.JValue

private[internal] object DataTypeParser {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  private val nonDecimalNameToType = {
    Seq(new NullType, new DateType, new TimestampType, new BinaryType, new IntegerType,
      new BooleanType, new LongType, new DoubleType, new FloatType, new ShortType, new ByteType,
      new StringType, new  CalendarIntervalType).map(t => t.getTypeName -> t).toMap
  }

  def fromJson(json: String): DataType = parseDataType(parse(json))

  def parseDataType(json: JValue): DataType = json match {
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
    ("metadata", _: JObject),
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      new StructField(name, parseDataType(dataType), nullable)
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      new StructField(name, parseDataType(dataType), nullable)
    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a field.")
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.sortBy(_._1))
      case _ => None
    }
  }
}
