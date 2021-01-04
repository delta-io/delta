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

package io.delta

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}

package object implicits {

  /**
   * Extends the DataFrameReader API by adding a delta function
   * Usage:
   * {{{
   * spark.read.delta(path)
   * }}}
   */
  implicit class DeltaDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def delta(path: String): DataFrame = {
      reader.format("delta").load(path)
    }
  }

  /**
   * Extends the DataStreamReader API by adding a delta function
   * Usage:
   * {{{
   * spark.readStream.delta(path)
   * }}}
   */
  implicit class DeltaDataStreamReader(val dataStreamReader: DataStreamReader) extends AnyVal {
    def delta(path: String): DataFrame = {
      dataStreamReader.format("delta").load(path)
    }
  }

  /**
   * Extends the DataFrameWriter API by adding a delta function
   * Usage:
   * {{{
   * df.write.delta(path)
   * }}}
   */
  implicit class DeltaDataFrameWriter[T](val dfWriter: DataFrameWriter[T]) extends AnyVal {
    def delta(output: String): Unit = {
      dfWriter.format("delta").save(output)
    }
  }

  /**
   * Extends the DataStreamWriter API by adding a delta function
   * Usage:
   * {{{
   * ds.writeStream.delta(path)
   * }}}
   */
  implicit class DeltaDataStreamWriter[T]
  (val dataStreamWriter: DataStreamWriter[T]) extends AnyVal {
    def delta(path: String): StreamingQuery = {
      dataStreamWriter.format("delta").start(path)
    }
  }
}
