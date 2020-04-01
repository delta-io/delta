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

package io.delta.hive

import java.io.{Closeable, File}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.tez.dag.api.TezConfiguration
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration
import org.apache.tez.test.MiniTezCluster

class HiveTezSuite extends HiveConnectorTest {

  override val engine: String = "tez"

  private var tezConf: Configuration = _

  // scalastyle:off
  /**
   * This method is based on
   * https://github.com/apache/hive/blob/c660cba003f9b7fff29db2202b375982a8c03450/shims/0.23/src/main/java/org/apache/hadoop/hive/shims/Hadoop23Shims.java#L406
   */
  // scalastyle:on
  override def createCluster(
      namenode: String,
      conf: Configuration,
      tempPath: File): Closeable = new Closeable {
    private val tez = {
      assert(sys.env("JAVA_HOME") != null, "Cannot find JAVA_HOME")
      val tez = new MiniTezCluster("hivetest", 2)
      conf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 256)
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256)
      conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 256)
      // Overrides values from the hive/tez-site.
      conf.setInt("hive.tez.container.size", 256)
      conf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 256)
      conf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 256)
      conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 24)
      conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB, 10)
      conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.4f)
      conf.set("fs.defaultFS", namenode)
      conf.set("tez.am.log.level", "DEBUG")
      conf.set(
        MRJobConfig.MR_AM_STAGING_DIR,
        new File(tempPath, "apps_staging_dir").getAbsolutePath)
      // - Set `spark.testing.reservedMemory` in the test so that Spark doesn't check the physical
      //   memory size. We are using a very small container and that's enough for testing.
      // - Reduce the partition number to 1 to reduce the memory usage of Spark because CircleCI has
      //   a small physical memory limit.
      // - Set the default timezone so that the answers of tests using timestamp is not changed when
      //   running in CircleCI.
      conf.set("tez.am.launch.cmd-opts",
        "-Dspark.testing.reservedMemory=0 " +
          "-Dspark.sql.shuffle.partitions=1 " +
          "-Dspark.databricks.delta.snapshotPartitions=1 " +
          "-Duser.timezone=America/Los_Angeles")
      conf.set("tez.task.launch.cmd-opts", "-Duser.timezone=America/Los_Angeles")
      // Disable disk health check and authorization
      conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 100.0F)
      conf.setBoolean(YarnConfiguration.NM_DISK_HEALTH_CHECK_ENABLE, false)
      conf.setBoolean("hadoop.security.authorization", false)
      tez.init(conf)
      tez.start()
      tezConf = tez.getConfig
      tez
    }

    override def close(): Unit = {
      tez.stop()
    }
  }

  // scalastyle:off
  /**
   * The method is based on
   * https://github.com/apache/hive/blob/c660cba003f9b7fff29db2202b375982a8c03450/shims/0.23/src/main/java/org/apache/hadoop/hive/shims/Hadoop23Shims.java#L446
   */
  // scalastyle:on
  override def setupConfiguration(conf: Configuration): Unit = {
    tezConf.asScala.foreach { e =>
      conf.set(e.getKey, e.getValue)
    }
    // Overrides values from the hive/tez-site.
    conf.setInt("hive.tez.container.size", 256)
    conf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 256)
    conf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 256)
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 24)
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB, 10)
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, 0.4f)
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true)
  }
}
