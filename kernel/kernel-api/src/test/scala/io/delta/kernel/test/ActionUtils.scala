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

package io.delta.kernel.test

import java.util.{Collections, Optional}

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.types.StructType

trait ActionUtils extends VectorTestUtils {
  def testMetadata(
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty,
      tblProps: Map[String, String] = Map.empty): Metadata = {
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      schema.toJson,
      schema,
      new ArrayValue() { // partitionColumns
        override def getSize: Int = partitionCols.size
        override def getElements: ColumnVector = stringVector(partitionCols)
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize: Int = tblProps.size
        override def getKeys: ColumnVector = stringVector(tblProps.toSeq.map(_._1))
        override def getValues: ColumnVector = stringVector(tblProps.toSeq.map(_._2))
      })
  }
}
