package io.delta.hive.test

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.mapred.{JobConf, MiniMRCluster}
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.network.util.JavaUtils

// TODO Yarn is using log4j2. Disable its verbose logs.
trait HiveTest extends FunSuite with BeforeAndAfterAll {
  private val tempPath = Files.createTempDirectory(this.getClass.getSimpleName).toFile

  private var driver: Driver = _
  private var mr: MiniMRCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val warehouseDir = new File(tempPath, "warehouse")
    val metastoreDir = new File(tempPath, "metastore_db")
    val conf = new HiveConf()

    // Disable schema verification and allow schema auto-creation in the
    // Derby database, in case the config for the metastore is set otherwise.
    // Without these settings, starting the client fails with
    // MetaException(message:Version information not found in metastore.)t
    conf.set("hive.metastore.schema.verification", "false")
    conf.set("datanucleus.schema.autoCreateAll", "true")
    conf.set(
      "javax.jdo.option.ConnectionURL",
      s"jdbc:derby:memory:;databaseName=${metastoreDir.getCanonicalPath};create=true")
    conf.set("hive.metastore.warehouse.dir", warehouseDir.getCanonicalPath)
    val fs = FileSystem.getLocal(conf)
    val jConf = new JobConf(conf)
    jConf.set("yarn.scheduler.capacity.root.queues", "default")
    jConf.set("yarn.scheduler.capacity.root.default.capacity", "100")
    jConf.setInt(MRJobConfig.MAP_MEMORY_MB, 512)
    jConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 512)
    jConf.setInt(MRJobConfig.MR_AM_VMEM_MB, 128)
    jConf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 512)
    jConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128)
    jConf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 512)
    mr = new MiniMRCluster(2, fs.getUri.toString, 1, null, null, jConf)

    val db = Hive.get(conf)
    SessionState.start(new CliSessionState(conf))
    driver = new Driver(conf)
  }

  override def afterAll() {
    if (mr != null) {
      mr.shutdown()
    }
    driver.close()
    driver.destroy()
    JavaUtils.deleteRecursively(tempPath)
    // TODO Remove leaked "target/MiniMRCluster-XXX" directories
    super.afterAll()
  }

  def runQuery(query: String): Seq[String] = {
    val response = driver.run(query)
    if (response.getResponseCode != 0) {
      throw new Exception(s"failed to run '$query': ${response.getErrorMessage}")
    }
    val result = new java.util.ArrayList[String]()
    if (driver.getResults(result)) {
      result.asScala
    } else {
      Nil
    }
  }
}
