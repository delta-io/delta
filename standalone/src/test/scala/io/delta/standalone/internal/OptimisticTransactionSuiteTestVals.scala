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

package io.delta.standalone.internal

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, Metadata => MetadataJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.expressions.{EqualTo, Literal}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

/**
 * By bundling these variables into a trait, we make it easier for other projects (specifically, the
 * Delta Standalone / Delta OSS compatibility project) to reuse these variables in concurrent write
 * tests.
 */
trait OptimisticTransactionSuiteTestVals {
  val addA = new AddFileJ("a", Collections.emptyMap(), 1, 1, true, null, null)
  val addB = new AddFileJ("b", Collections.emptyMap(), 1, 1, true, null, null)

  val removeA = RemoveFileJ.builder("a").deletionTimestamp(4L).build()
  val removeA_time5 = RemoveFileJ.builder("a").deletionTimestamp(5L).build()

  val addA_partX1 = new AddFileJ("a", Map("x" -> "1").asJava, 1, 1, true, null, null)
  val addA_partX2 = new AddFileJ("a", Map("x" -> "2").asJava, 1, 1, true, null, null)
  val addB_partX1 = new AddFileJ("b", Map("x" -> "1").asJava, 1, 1, true, null, null)
  val addB_partX3 = new AddFileJ("b", Map("x" -> "3").asJava, 1, 1, true, null, null)
  val addC_partX4 = new AddFileJ("c", Map("x" -> "4").asJava, 1, 1, true, null, null)

  val schema = new StructType(Array(new StructField("x", new IntegerType())))
  val colXEq1Filter = new EqualTo(schema.column("x"), Literal.of(1))
  val metadata_colX = MetadataJ.builder().schema(schema).build()
  val metadata_partX =
    MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build()
}
