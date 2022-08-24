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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class DeltaImplicitsSuite extends SparkFunSuite with SharedSparkSession {

  private def testImplict(name: String, func: => Unit): Unit = {
    test(name) {
      func
    }
  }

  import org.apache.spark.sql.delta.implicits._

  testImplict("int", intEncoder)
  testImplict("long", longEncoder)
  testImplict("string", stringEncoder)
  testImplict("longLong", longLongEncoder)
  testImplict("stringLong", stringLongEncoder)
  testImplict("stringString", stringStringEncoder)
  testImplict("javaLong", javaLongEncoder)
  testImplict("singleAction", singleActionEncoder)
  testImplict("addFile", addFileEncoder)
  testImplict("removeFile", removeFileEncoder)
  testImplict("serializableFileStatus", serializableFileStatusEncoder)
  testImplict("indexedFile", indexedFileEncoder)
  testImplict("addFileWithIndex", addFileWithIndexEncoder)
  testImplict("deltaHistoryEncoder", deltaHistoryEncoder)
  testImplict("historyCommitEncoder", historyCommitEncoder)
  testImplict("snapshotStateEncoder", snapshotStateEncoder)

  testImplict("RichAddFileSeq: toDF", Seq(AddFile("foo", Map.empty, 0, 0, true)).toDF(spark))
  testImplict("RichAddFileSeq: toDS", Seq(AddFile("foo", Map.empty, 0, 0, true)).toDS(spark))
  testImplict("RichStringSeq: toDF", Seq("foo").toDF(spark))
  testImplict("RichStringSeq: toDF(col)", Seq("foo").toDF(spark, "str"))
  testImplict("RichIntSeq: toDF", Seq(1).toDF(spark))
  testImplict("RichIntSeq: toDF(col)", Seq(1).toDF(spark, "int"))
}
