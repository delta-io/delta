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
package io.delta.spark.internal.v2.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;

/** Public factory methods for Delta's package-private Spark DataSource V2 read internals. */
public final class DeltaV2Reads {

  private DeltaV2Reads() {}

  public static PartitionReaderFactory newReaderFactory(
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc, boolean supportsColumnar) {
    return new DeltaV2ReaderFactory(readFunc, supportsColumnar);
  }
}
