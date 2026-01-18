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

import java.io.{File}
import java.nio.charset.StandardCharsets

import io.delta.storage.HDFSLogStore
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

class DuplicatingListLogStore(defaultHadoopConf: Configuration)
  extends HDFSLogStore(defaultHadoopConf) {

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    import scala.collection.JavaConverters._
    val list = super.listFrom(path, hadoopConf).asScala.toSeq
    // The first listing if directory will be listed twice to mimic the WASBS Log Store
    if (!list.isEmpty && list.head.isDirectory) {
      (Seq(list.head) ++ list).toIterator.asJava
    } else {
      list.toIterator.asJava
    }
  }
}

class DuplicatingListLogStoreSuite extends SharedSparkSession with DeltaSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.databricks.tahoe.logStore.class",
      classOf[DuplicatingListLogStore].getName)
  }

  def pathExists(deltaLog: DeltaLog, filePath: String): Boolean = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    fs.exists(DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, filePath))
  }

  test("vacuum should handle duplicate listing") {
    withTempDir { dir =>
      // create cdc file (lexicographically < _delta_log)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val deltaTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      val cdcDir = new File(new Path(dir.getAbsolutePath, "_change_data").toString)
      cdcDir.mkdir()
      val cdcPath = new File(
        new Path(cdcDir.getAbsolutePath, "dupFile").toString)
      Files.write("test", cdcPath, StandardCharsets.UTF_8)

      require(pathExists(deltaLog, cdcPath.toString))
      require(pathExists(deltaLog, cdcDir.toString))

      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        deltaTable.vacuum(0)

        // check if path doesn't exists
        assert(!pathExists(deltaLog, cdcPath.toString))

        // to delete directories
        deltaTable.vacuum(0)
        assert(!pathExists(deltaLog, cdcDir.toString))
      }
    }
  }
}
