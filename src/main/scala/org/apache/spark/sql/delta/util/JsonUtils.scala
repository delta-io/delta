/*
 * Copyright 2019 Databricks, Inc.
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
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/** Useful json functions used around the Delta codebase. */
object JsonUtils {
  /** Used to convert between classes and JSON. */
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def toPrettyJson[T: Manifest](obj: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json)
  }
}
