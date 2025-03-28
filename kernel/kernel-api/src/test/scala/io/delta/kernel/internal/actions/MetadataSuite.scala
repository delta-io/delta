/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.actions

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils.buildColumnVector
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class MetadataSuite extends AnyFunSuite {

  test("withMergedConfig upserts values") {
    val metadata = testMetadata(Map("a" -> "b", "f" -> "g"))

    val newMetadata = metadata.withMergedConfiguration(Map("a" -> "c", "d" -> "f").asJava)

    assert(newMetadata.getConfiguration.equals(Map("a" -> "c", "d" -> "f", "f" -> "g").asJava))
  }

  test("withConfiguration replaces values") {
    val metadata = testMetadata(Map("a" -> "b", "f" -> "g"))

    val newMetadata = metadata.withConfiguration(Map("a" -> "c", "d" -> "f").asJava)

    assert(newMetadata.getConfiguration.equals(Map("a" -> "c", "d" -> "f").asJava))
  }

  def testMetadata(tblProps: Map[String, String] = Map.empty): Metadata = {
    val testSchema = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", StringType.STRING)
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      testSchema.toJson,
      testSchema,
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = tblProps.size
        override def getKeys: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._1).asJava, StringType.STRING)
        override def getValues: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._2).asJava, StringType.STRING)
      })
  }

}
