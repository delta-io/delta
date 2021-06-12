package org.apache.spark.sql.delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class ExternalPostCommitHookSuite extends DeltaGenerateSymlinkManifestSuiteBase
  with DeltaSQLCommandTest {

  import testImplicits._

  test("generate manifest with postCommitHookClass") {
    withTempDir { tablePath =>
      tablePath.delete()
      withSQLConf(
        "spark.databricks.delta.properties.defaults.postCommitHookClass" ->
          classOf[ExternalPostCommitHook].getName) {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
          .write.format("delta").mode("overwrite").save(tablePath.toString)

        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
      }
    }
  }
}

class ExternalPostCommitHook extends PostCommitHook {
  /** A user friendly name for the hook for error reporting purposes. */
  override val name: String = "ExternalPostCommitHook"

  /** Executes the hook. */
  override def run(
    spark: SparkSession,
    txn: OptimisticTransactionImpl,
    committedActions: Seq[Action]): Unit = {

    // Create a Delta table and call the scala api for generating manifest files
    spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE delta.`${txn.deltaLog.dataPath}`")
  }
}