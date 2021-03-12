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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.actions.Format

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.StructType

trait DeltaSourceSuiteBase extends StreamTest {
  protected def withMetadata(
      deltaLog: DeltaLog,
      schema: StructType,
      format: String = "parquet",
      tableId: Option[String] = None): Unit = {
    val txn = deltaLog.startTransaction()
    val baseMetadata = tableId.map { tId => txn.metadata.copy(id = tId) }.getOrElse(txn.metadata)
    txn.commit(baseMetadata.copy(
      schemaString = schema.json,
      format = Format(format)
    ) :: Nil, DeltaOperations.ManualUpdate)
  }

  object AddToReservoir {
    def apply(path: File, data: DataFrame): AssertOnQuery =
      AssertOnQuery { _ =>
        data.write.format("delta").mode("append").save(path.getAbsolutePath)
        true
      }
  }

  object CheckProgress {
    def apply(rowsPerBatch: Seq[Int]): AssertOnQuery =
      Execute { q =>
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === rowsPerBatch.size, "Expected batches don't match")
        progress.zipWithIndex.foreach { case (p, i) =>
          assert(p.numInputRows === rowsPerBatch(i), s"Expected rows in batch $i does not match ")
        }
      }
  }
}
