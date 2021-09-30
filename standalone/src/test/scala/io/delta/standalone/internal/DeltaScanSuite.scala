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

package io.delta.standalone.internal

import scala.collection.JavaConverters._

import io.delta.standalone.expressions.{And, EqualTo, LessThan, Literal}
import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.scan.FilteredDeltaScanImpl
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

class DeltaScanSuite extends FunSuite {

  private val schema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true),
    new StructField("col3", new IntegerType(), true),
    new StructField("col4", new IntegerType(), true)
  ))

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true)
  ))

  private val files = (1 to 10).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true)
  }

  private val metadataConjunct = new EqualTo(schema.column("col1"), Literal.of(0))
  private val dataConjunct = new EqualTo(schema.column("col3"), Literal.of(5))

  test("properly splits metadata (pushed) and data (residual) predicates") {
    val mixedConjunct = new LessThan(schema.column("col2"), schema.column("col4"))
    val filter = new And(new And(metadataConjunct, dataConjunct), mixedConjunct)
    val scan = new FilteredDeltaScanImpl(files, filter, partitionSchema)

    assert(scan.getPushedPredicate.get == metadataConjunct)
    assert(scan.getResidualPredicate.get == new And(dataConjunct, mixedConjunct))
  }

  test("filtered scan with a metadata (pushed) conjunct should return matched files") {
    val filter = new And(metadataConjunct, dataConjunct)
    val scan = new FilteredDeltaScanImpl(files, filter, partitionSchema)

    assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) ==
      files.filter(_.partitionValues("col1").toInt == 0))
    assert(scan.getFilesScala == files.filter(_.partitionValues("col1").toInt == 0))

    assert(scan.getPushedPredicate.get == metadataConjunct)
    assert(scan.getResidualPredicate.get == dataConjunct)
  }

  test("filtered scan with only data (residual) predicate should return all files") {
    val filter = dataConjunct
    val scan = new FilteredDeltaScanImpl(files, filter, partitionSchema)

    assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) == files)
    assert(scan.getFilesScala == files)

    assert(!scan.getPushedPredicate.isPresent)
    assert(scan.getResidualPredicate.get == filter)
  }
}
