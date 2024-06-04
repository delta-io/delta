/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.StructType

/**
 * Extends the streaming testing framework from [[StreamTest]] to provide testing helpers
 * for Delta streaming sources.
 */
trait DeltaStreamTest extends StreamTest with DeltaStreamActions {

  /** Convenience helper to create a streaming df from a delta table path. */
  def readStreamFromPath(path: File, options: Map[String, String] = Map.empty): DataFrame =
    spark.readStream.options(options).format("delta").load(path.getCanonicalPath)
}

/**
 * Defines Delta-specific test actions that can be passed to the `testStream` method.
 */
trait DeltaStreamActions { self: StreamTest =>
  /**
   * Overwrite the Delta table at the given path to change its schema while preserving its history
   * and keeping the same table ID.
   */
  object OverwriteTable {
    def apply(path: File, schema: StructType): AssertOnQuery =
      Execute("ChangeSchema") { _ =>
        spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .option("overwriteSchema", "true")
          .save(path.getCanonicalPath)
      }
  }

  /**
   * Check that the schema of the Delta table at the given path matches the expected schema.
   */
  object CheckSchema {
    def apply(deltaPath: File, expectedSchema: StructType): AssertOnQuery =
      Execute("CheckSchema") { q =>
        q.processAllAvailable()
        val schema = spark.read.format("delta").load(deltaPath.getCanonicalPath).schema
        assert(schema === expectedSchema)
      }
  }

  /**
   * Check that the stream fails because of a schema change.
   */
  object ExpectSchemaChangeFailure {
    def apply(readSchema: String, dataSchema: String, isRetryable: Boolean): StreamAction = {
      def matchSchemaStr(schema: String): String = StructType.fromDDL(schema).map { field =>
        s"${field.name}: ${field.dataType.typeName}"
      }.mkString("(?s).*", ".*", ".*")

      def checkError(exception: SparkThrowable): Unit =
        checkErrorMatchPVals(
          exception = exception,
          errorClass = "DELTA_SCHEMA_CHANGED_WITH_VERSION",
          parameters = Map(
            "version" -> ".*",
            "readSchema" -> matchSchemaStr(readSchema),
            "dataSchema" -> matchSchemaStr(dataSchema)
          )
        )

      ExpectFailure[DeltaIllegalStateException] { ex =>
        checkError(ex.asInstanceOf[DeltaIllegalStateException])
      }
    }
  }
}
