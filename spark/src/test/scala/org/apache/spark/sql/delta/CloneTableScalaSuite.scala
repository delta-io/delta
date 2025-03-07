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

import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.hadoop.fs.Path

class CloneTableScalaSuite extends CloneTableSuiteBase
    with AnalysisHelper
    with DeltaColumnMappingTestUtils {

  import testImplicits._

  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      isShallow: Boolean,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val table = if (sourceIsTable) {
      io.delta.tables.DeltaTable.forName(spark, source)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, source)
    }

    if (versionAsOf.isDefined) {
      table.cloneAtVersion(versionAsOf.get,
        target, isShallow = isShallow, replace = isReplace, tableProperties)
    } else if (timestampAsOf.isDefined) {
      table.cloneAtTimestamp(timestampAsOf.get,
        target, isShallow = isShallow, replace = isReplace, tableProperties)
    } else {
      table.clone(target, isShallow = isShallow, replace = isReplace, tableProperties)
    }
  }
  // scalastyle:on argcount

  testAllClones("cloneAtVersion API") { (source, target, isShallow) =>
    spark.range(5).write.format("delta").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)

    val sourceTbl = io.delta.tables.DeltaTable.forPath(source)
    assert(spark.read.format("delta").load(source).count() === 15)

    runAndValidateClone(source, target, isShallow, sourceVersion = Some(0)) {
      () => {
        sourceTbl.cloneAtVersion(0, target, isShallow)
      }
    }
  }

  test("deep clone not supported yet") {
    withSourceTargetDir { (source, clone) =>
      checkError(
        intercept[DeltaIllegalArgumentException] {
          val df1 = Seq(1, 2, 3, 4, 5).toDF("id").withColumn("part", 'id % 2)
          val df2 = Seq(8, 9, 10).toDF("id").withColumn("part", 'id % 2)
          df1.write.format("delta").partitionBy("part").mode("append").save(source)
          df2.write.format("delta").mode("append").save(source)

          runAndValidateClone(source, clone, isShallow = false)()
        },
        "DELTA_UNSUPPORTED_DEEP_CLONE"
      )
    }
  }

  testAllClones("clone API") { (source, target, isShallow) =>
    spark.range(5).write.format("delta").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)

    val sourceTbl = io.delta.tables.DeltaTable.forPath(source)
    assert(spark.read.format("delta").load(source).count() === 15)

    runAndValidateClone(source, target, isShallow) {
      () => {
        sourceTbl.clone(target, isShallow)
      }
    }
  }

  testAllClones("cloneAtTimestamp API") { (source, target, isShallow) =>
    spark.range(5).write.format("delta").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)

    val sourceTbl = io.delta.tables.DeltaTable.forPath(source)
    assert(spark.read.format("delta").load(source).count() === 15)

    val desiredTime = "1996-01-12"

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val time = format.parse(desiredTime).getTime

    val path = new Path(source + "/_delta_log/00000000000000000000.json")
    // scalastyle:off deltahadoopconfiguration
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    fs.setTimes(path, time, 0)
    if (coordinatedCommitsEnabledInTests) {
      InCommitTimestampTestUtils.overwriteICTInDeltaFile(
        DeltaLog.forTable(spark, source),
        path,
        Some(time))
    }

    runAndValidateClone(source, target, isShallow, sourceTimestamp = Some(desiredTime)) {
      () => {
        sourceTbl.cloneAtTimestamp(desiredTime, target, isShallow)
      }
    }
  }
}

class CloneTableScalaIdColumnMappingSuite
    extends CloneTableScalaSuite
    with CloneTableColumnMappingSuiteBase
    with DeltaColumnMappingEnableIdMode {

  override protected def runOnlyTests: Seq[String] = super.runOnlyTests ++ Seq(
    "cloneAtVersion API",
    "clone API",
    "cloneAtTimestamp API"
  )
}

class CloneTableScalaNameColumnMappingSuite
    extends CloneTableScalaSuite
    with CloneTableColumnMappingNameSuiteBase
    with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests: Seq[String] = super.runOnlyTests ++ Seq(
    "cloneAtVersion API",
    "clone API",
    "cloneAtTimestamp API"
  )
}
