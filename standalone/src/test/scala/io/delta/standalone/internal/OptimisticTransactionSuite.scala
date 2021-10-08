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

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{Protocol, AddFile => AddFileJ, Metadata => MetadataJ, RemoveFile => RemoveFileJ, SetTransaction => SetTransactionJ}
import io.delta.standalone.expressions.{EqualTo, Literal}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

class OptimisticTransactionSuite extends OptimisticTransactionSuiteBase {
  private val addA = new AddFileJ("a", Collections.emptyMap(), 1, 1, true, null, null)
  private val addB = new AddFileJ("b", Collections.emptyMap(), 1, 1, true, null, null)

  private val addA_partX1 = new AddFileJ("a", Map("x" -> "1").asJava, 1, 1, true, null, null)
  private val addA_partX2 = new AddFileJ("a", Map("x" -> "2").asJava, 1, 1, true, null, null)
  private val schema = new StructType(Array(new StructField("x", new IntegerType())))

  /* ************************** *
   * Allowed concurrent actions *
   * ************************** */

  check(
    "append / append",
    conflicts = false,
    reads = Seq(t => t.metadata()),
    concurrentWrites = Seq(addA),
    actions = Seq(addB))

  check(
    "disjoint txns",
    conflicts = false,
    reads = Seq(
      t => t.txnVersion("t1")
    ),
    concurrentWrites = Seq(
      new SetTransactionJ("t2", 0, java.util.Optional.of(1234L))),
    actions = Nil)

  check(
    "disjoint delete / read",
    conflicts = false,
    setup = Seq(
      MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build(),
      addA_partX2
    ),
    reads = Seq(
      t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
    ),
    concurrentWrites = Seq(
      RemoveFileJ.builder("a").deletionTimestamp(4L).build()
    ),
    actions = Seq()
  )

  check(
    "disjoint add / read",
    conflicts = false,
    setup = Seq(
      MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build()
    ),
    reads = Seq(
      t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
    ),
    concurrentWrites = Seq(
      addA_partX2
    ),
    actions = Seq()
  )

  check(
    "add / read + no write",  // no write = no real conflicting change even though data was added
    conflicts = false,        // so this should not conflict
    setup = Seq(
      MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build()
    ),
    reads = Seq(
      t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
    ),
    concurrentWrites = Seq(addA_partX1),
    actions = Seq())

  /* ***************************** *
   * Disallowed concurrent actions *
   * ***************************** */

  check(
    "delete / delete",
    conflicts = true,
    reads = Nil,
    concurrentWrites = Seq(
      RemoveFileJ.builder("a").deletionTimestamp(4L).build()),
    actions = Seq(
      RemoveFileJ.builder("a").deletionTimestamp(4L).build()))

  check(
    "add / read + write",
    conflicts = true,
    setup = Seq(
      MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build()
    ),
    reads = Seq(
      t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
    ),
    concurrentWrites = Seq(addA_partX1),
    actions = Seq(addA_partX1),
    // commit info should show operation as "Manual Update", because that's the operation used by
    // the harness
    errorMessageHint = Some("[x=1]" :: "Manual Update" :: Nil))

  check(
    "delete / read",
    conflicts = true,
    setup = Seq(
      MetadataJ.builder().schema(schema).partitionColumns(Seq("x").asJava).build(),
      addA_partX1
    ),
    reads = Seq(
      t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
    ),
    concurrentWrites = Seq(
      RemoveFileJ.builder("a").deletionTimestamp(4L).build()
    ),
    actions = Seq(),
    errorMessageHint = Some("a in partition [x=1]" :: "Manual Update" :: Nil))

  check(
    "schema change",
    conflicts = true,
    reads = Seq(
      t => t.metadata
    ),
    concurrentWrites = Seq(MetadataJ.builder().build()),
    actions = Nil)

  check(
    "conflicting txns",
    conflicts = true,
    reads = Seq(
      t => t.txnVersion("t1")
    ),
    concurrentWrites = Seq(
      new SetTransactionJ("t1", 0, java.util.Optional.of(1234L))
    ),
    actions = Nil)

  check(
    "upgrade / upgrade",
    conflicts = true,
    reads = Seq(
      t => t.metadata
    ),
    concurrentWrites = Seq(
      new Protocol()),
    actions = Seq(
      new Protocol()))

  // TODO: taint whole table

  // TODO: taint whole table + concurrent remove

  // TODO: initial commit without metadata should fail

  // TODO: initial commit with multiple metadata actions should fail

  // TODO: AddFile with different partition schema compared to metadata should fail

  // TODO: isolation level shouldn't be null ?
}
