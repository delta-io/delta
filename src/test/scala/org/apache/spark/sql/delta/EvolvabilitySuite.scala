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

import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.DebugFilesystem // EDGE
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class EvolvabilitySuite extends EvolvabilitySuiteBase with SQLTestUtils {

  import testImplicits._

  test("delta 0.1.0") {
    testEvolvability("src/test/resources/delta/delta-0.1.0")
  }

  test("delta 0.1.0 - case sensitivity enabled") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      testEvolvability("src/test/resources/delta/delta-0.1.0")
    }
  }

  testQuietly("future proofing against new features") {

    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    deltaLog.store.write(new Path(deltaLog.logPath, "00000000000000000001.json"),
      Iterator("""{"some_new_feature":{"a":1}}"""))

    // Shouldn't fail here
    deltaLog.update()

    val sq = spark.readStream.format("delta").load(tempDir.toString)
      .groupBy()
      .count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Also shouldn't fail
    sq.processAllAvailable()
    Seq(1, 2, 3).toDF().write.format("delta").mode("append").save(tempDir)
    sq.processAllAvailable()
    deltaLog.store.write(new Path(deltaLog.logPath, "00000000000000000003.json"),
      Iterator("""{"some_new_feature":{"a":1}}"""))
    sq.processAllAvailable()
    sq.stop()
  }

  test("serialized partition values must contain null values") {
    val tempDir = Utils.createTempDir().toString
    val df1 = spark.range(5).withColumn("part", typedLit[String](null))
    val df2 = spark.range(5).withColumn("part", typedLit("1"))
    df1.union(df2).coalesce(1).write.partitionBy("part").format("delta").save(tempDir)

    // Clear the cache
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tempDir)

    val dataThere = deltaLog.snapshot.allFiles.collect().forall { addFile =>
      if (!addFile.partitionValues.contains("part")) {
        fail(s"The partition values: ${addFile.partitionValues} didn't contain the column 'part'.")
      }
      val value = addFile.partitionValues("part")
      value === null || value === "1"
    }

    assert(dataThere, "Partition values didn't match with null or '1'")

    // Check serialized JSON as well
    val contents = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, 0L))
    assert(contents.exists(_.contains(""""part":null""")), "null value should be written in json")
  }

  testQuietly("parse old version CheckpointMetaData") {
    assert(JsonUtils.mapper.readValue[CheckpointMetaData]("""{"version":1,"size":1}""")
      == CheckpointMetaData(1, 1, None))
  }
}
