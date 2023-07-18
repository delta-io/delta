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
package io.delta.kernel

import java.sql.Timestamp

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  val expectedResult = Set(
    (0, Timestamp.valueOf("2020-01-01 08:09:10.001"), Timestamp.valueOf("2020-02-01 08:09:10")),
    (1, Timestamp.valueOf("2021-10-01 08:09:20"), Timestamp.valueOf("1999-01-01 09:00:00")),
    (2, Timestamp.valueOf("2021-10-01 08:09:20"), Timestamp.valueOf("2000-01-01 09:00:00")))

  for (timestampType <- Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
    test(s"end-to-end usage: timestamp table with parquet timestamp format $timestampType") {
    // kernel expects a fully qualified path
    val path = "file:" + goldenTablePath("kernel-timestamp-" + timestampType)
    val result = readTable(path, new Configuration()) { row =>
      // Convert from micros to millis
      (row.getInt(0), new Timestamp(row.getLong(1)/1000), new Timestamp(row.getLong(2)/1000))
    }
    assert(result.toSet == expectedResult)
    }
  }
}
