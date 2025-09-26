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

import java.util.{Collections, Map => JMap, Optional}
import java.util.function.Supplier

import scala.collection.JavaConverters._

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.internal.actions.{CommitInfo, DomainMetadata, Metadata, Protocol}
import io.delta.kernel.internal.util.Tuple2
import io.delta.kernel.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}

/**
 * Test fixtures including factory methods and constants for creating test objects with sensible
 * defaults.
 */
trait TestFixtures extends ActionUtils {

  /** All simple data type used in parameterized tests where type is one of the test dimensions. */
  val PRIMITIVE_TYPES = Set(
    BooleanType.BOOLEAN,
    ByteType.BYTE,
    ShortType.SHORT,
    IntegerType.INTEGER,
    LongType.LONG,
    FloatType.FLOAT,
    DoubleType.DOUBLE,
    DateType.DATE,
    TimestampType.TIMESTAMP,
    TimestampNTZType.TIMESTAMP_NTZ,
    StringType.STRING,
    BinaryType.BINARY,
    new DecimalType(10, 5))

  val NESTED_TYPES: Set[DataType] = Set(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER))

  /** All types. Used in parameterized tests where type is one of the test dimensions. */
  val ALL_TYPES: Set[DataType] = PRIMITIVE_TYPES ++ NESTED_TYPES

  def createCommitMetadata(
      version: Long,
      logPath: String = "/fake/_delta_log",
      commitInfo: CommitInfo = testCommitInfo(),
      commitDomainMetadatas: List[DomainMetadata] = List.empty,
      committerProperties: Supplier[JMap[String, String]] = () => Collections.emptyMap(),
      readPandMOpt: Optional[Tuple2[Protocol, Metadata]] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty()): CommitMetadata = {
    new CommitMetadata(
      version,
      logPath,
      commitInfo,
      commitDomainMetadatas.asJava,
      committerProperties,
      readPandMOpt,
      newProtocolOpt,
      newMetadataOpt)
  }

}
