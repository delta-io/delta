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

import com.databricks.sql.managedcatalog.{ManagedCatalogSessionCatalog, TestMode}
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.redirect.{
  DropRedirectInProgress,
  EnableRedirectInProgress,
  NoRedirectRule,
  PathBasedRedirectSpec,
  RedirectReaderWriter,
  RedirectReady,
  RedirectSpec,
  RedirectState,
  RedirectWriterOnly,
  TableRedirect,
  TableRedirectConfiguration
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.commons.text.StringEscapeUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.test.SharedSparkSession

class TableRedirectSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite
  with DeltaCheckpointTestUtils
  with DeltaSQLTestUtils {

  private def validateState(
      deltaLog: DeltaLog,
      redirectState: RedirectState,
      destTablePath: File,
      feature: TableRedirect
  ): Unit = {
    val snapshot = deltaLog.update()
    assert(feature.isFeatureSet(snapshot.metadata))
    val redirectConfig = feature.getRedirectConfiguration(snapshot.metadata).get
    val protocol = snapshot.protocol
    if (feature != RedirectWriterOnly) {
      assert(protocol.readerFeatureNames.contains(RedirectReaderWriterFeature.name))
      assert(protocol.writerFeatureNames.contains(RedirectReaderWriterFeature.name))
    } else {
      assert(!protocol.readerFeatureNames.contains(RedirectWriterOnlyFeature.name))
      assert(protocol.writerFeatureNames.contains(RedirectWriterOnlyFeature.name))
    }
    assert(redirectConfig.redirectState == redirectState)
    assert(redirectConfig.`type` == PathBasedRedirectSpec.REDIRECT_TYPE)
    val expectedSpecValue = s"""{"tablePath":"${destTablePath.getCanonicalPath}"}"""
    assert(redirectConfig.specValue == expectedSpecValue)
    val redirectSpec = redirectConfig.spec.asInstanceOf[PathBasedRedirectSpec]
    assert(redirectSpec.tablePath == destTablePath.getCanonicalPath)
  }

  private def validateRemovedState(deltaLog: DeltaLog, feature: TableRedirect): Unit = {
    val snapshot = deltaLog.update()
    val protocol = snapshot.protocol
    assert(!feature.isFeatureSet(snapshot.metadata))
    if (feature != RedirectWriterOnly) {
      assert(protocol.readerFeatureNames.contains(RedirectReaderWriterFeature.name))
      assert(protocol.writerFeatureNames.contains(RedirectReaderWriterFeature.name))
    } else {
      assert(!protocol.readerFeatureNames.contains(RedirectWriterOnlyFeature.name))
      assert(protocol.writerFeatureNames.contains(RedirectWriterOnlyFeature.name))
    }
  }

  def redirectTest(
      label: String,
      accessSource: Boolean = false
  )(f: (DeltaLog, File, File, CatalogTable) => Unit): Unit = {
    test(s"basic table redirect: $label") {
      withTempDir { sourceTablePath =>
        withTempDir { destTablePath =>
          withSQLConf(DeltaSQLConf.ENABLE_TABLE_REDIRECT_FEATURE.key -> accessSource.toString) {
            withTable("t1", "t2") {
              sql(s"CREATE external TABLE t1(c0 long) USING delta LOCATION '$sourceTablePath';")
              val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
              val deltaLog = DeltaLog.forTable(spark, new Path(sourceTablePath.getCanonicalPath))
              f(deltaLog, sourceTablePath, destTablePath, catalogTable)
            }
          }
        }
      }
    }
  }

  Seq(RedirectReaderWriter, RedirectWriterOnly).foreach { feature =>
    val featureName = feature.config.key
    Seq(true, false).foreach { hasCatalogTable =>
      redirectTest(s"basic redirect: $featureName - " +
        s"hasCatalogTable: $hasCatalogTable") { case (deltaLog, _, dest, catalogTable) =>
        val snapshot = deltaLog.update()
        assert(!feature.isFeatureSet(snapshot.metadata))
        val redirectSpec = new PathBasedRedirectSpec(dest.getCanonicalPath)
        val catalogTableOpt = if (hasCatalogTable) Some(catalogTable) else None
        val redirectType = PathBasedRedirectSpec.REDIRECT_TYPE
        // Step-1: Initiate table redirection and set to EnableRedirectInProgress state.
        feature.add(deltaLog, catalogTableOpt, redirectType, redirectSpec)
        validateState(deltaLog, EnableRedirectInProgress, dest, feature)
        // Step-2: Complete table redirection and set to RedirectReady state.
        feature.update(deltaLog, catalogTableOpt, RedirectReady, redirectSpec)
        validateState(deltaLog, RedirectReady, dest, feature)
        // Step-3: Start dropping table redirection and set to DropRedirectInProgress state.
        feature.update(deltaLog, catalogTableOpt, DropRedirectInProgress, redirectSpec)
        validateState(deltaLog, DropRedirectInProgress, dest, feature)
        // Step-4: Finish dropping table redirection and remove the property completely.
        feature.remove(deltaLog, catalogTableOpt)
        validateRemovedState(deltaLog, feature)
        // Step-5: Initiate table redirection and set to EnableRedirectInProgress state one
        // more time.
        withTempDir { destTablePath2 =>
          val redirectSpec = new PathBasedRedirectSpec(destTablePath2.getCanonicalPath)
          feature.add(deltaLog, catalogTableOpt, redirectType, redirectSpec)
          validateState(deltaLog, EnableRedirectInProgress, destTablePath2, feature)
          // Step-6: Finish dropping table redirection and remove the property completely.
          feature.remove(deltaLog, catalogTableOpt)
          validateRemovedState(deltaLog, feature)
        }
      }

      redirectTest(s"Redirect $featureName: empty no redirect rules - " +
        s"hasCatalogTable: $hasCatalogTable") {
        case (deltaLog, source, dest, catalogTable) =>
          val snapshot = deltaLog.update()
          assert(!feature.isFeatureSet(snapshot.metadata))
          val redirectSpec = new PathBasedRedirectSpec(dest.getCanonicalPath)
          val catalogTableOpt = if (hasCatalogTable) Some(catalogTable) else None
          val redirectType = PathBasedRedirectSpec.REDIRECT_TYPE
          // 0. Initialize table redirection by setting table to EnableRedirectInProgress state.
          feature.add(deltaLog, catalogTableOpt, redirectType, redirectSpec)
          validateState(deltaLog, EnableRedirectInProgress, dest, feature)

          // 1. INSERT should hit DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE because table is in
          //    EnableRedirectInProgress, which doesn't allow any DML and DDL.
          val exception1 = intercept[DeltaIllegalStateException] {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          }
          assert(exception1.getErrorClass == "DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE")

          // 2. DDL should hit DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE because table is in
          //    EnableRedirectInProgress, which doesn't allow any DML and DDL.
          val exception2 = intercept[DeltaIllegalStateException] {
            sql(s"alter table delta.`$source` add column c3 long")
          }
          assert(exception2.getErrorClass == "DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE")

          // 3. Move to RedirectReady state.
          feature.update(deltaLog, catalogTableOpt, RedirectReady, redirectSpec)

          // 4. INSERT should hit DELTA_NO_REDIRECT_RULES_VIOLATED since the
          //    no-redirect-rules is empty.
          validateState(deltaLog, RedirectReady, dest, feature)
          val exception3 = intercept[DeltaIllegalStateException] {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          }
          assert(exception3.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 5. DDL should hit DELTA_NO_REDIRECT_RULES_VIOLATED since the
          //    no-redirect-rules is empty.
          val exception4 = intercept[DeltaIllegalStateException] {
            sql(s"alter table delta.`$source` add column c3 long")
          }
          assert(exception4.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 6. Move to DropRedirectInProgress state.
          feature.update(deltaLog, catalogTableOpt, DropRedirectInProgress, redirectSpec)

          // 7. INSERT should hit DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE because table is in
          //    DropRedirectInProgress, which doesn't allow any DML and DDL.
          validateState(deltaLog, DropRedirectInProgress, dest, feature)
          val exception5 = intercept[DeltaIllegalStateException] {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          }
          assert(exception5.getErrorClass == "DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE")

          // 8. DDL should hit DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE because table is in
          //    DropRedirectInProgress, which doesn't allow any DML and DDL.
          val exception6 = intercept[DeltaIllegalStateException] {
            sql(s"alter table delta.`$source` add column c3 long")
          }
          assert(exception6.getErrorClass == "DELTA_COMMIT_INTERMEDIATE_REDIRECT_STATE")
      }

      redirectTest(s"Redirect $featureName: no redirect rules - " +
        s"hasCatalogTable: $hasCatalogTable") {
        case (deltaLog, source, dest, catalogTable) =>
          val snapshot = deltaLog.update()
          assert(!feature.isFeatureSet(snapshot.metadata))
          val redirectSpec = new PathBasedRedirectSpec(dest.getCanonicalPath)
          val catalogTableOpt = if (hasCatalogTable) Some(catalogTable) else None
          val redirectType = PathBasedRedirectSpec.REDIRECT_TYPE
          sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          feature.add(deltaLog, catalogTableOpt, redirectType, redirectSpec)
          validateState(deltaLog, EnableRedirectInProgress, dest, feature)
          // 1. Move table redirect to RedirectReady state with no redirect rules that
          // allows WRITE, DELETE, UPDATE.
          var noRedirectRules = Set(
            NoRedirectRule(
              appName = None,
              allowedOperations = Set(
                DeltaOperations.Write(SaveMode.Append).name,
                DeltaOperations.Delete(Seq.empty).name,
                DeltaOperations.Update(None).name
              )
            )
          )
          feature.update(deltaLog, catalogTableOpt, RedirectReady, redirectSpec, noRedirectRules)
          validateState(deltaLog, RedirectReady, dest, feature)
          sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          sql(s"update delta.`$source` set c0 = 100")
          sql(s"delete from delta.`$source` where c0 = 1")

          // 2. Move table redirect to RedirectReady state with no-redirect-rules that
          //    allows UPDATE.
          noRedirectRules = Set(
            NoRedirectRule(
              appName = None, allowedOperations = Set(DeltaOperations.Update(None).name)
            )
          )
          feature.update(deltaLog, catalogTableOpt, RedirectReady, redirectSpec, noRedirectRules)
          validateState(deltaLog, RedirectReady, dest, feature)
          // 2.1. WRITE should be aborted because no-redirect-rules only allow UPDATE.
          val exception1 = intercept[DeltaIllegalStateException] {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          }
          assert(exception1.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 2.2. UPDATE should pass because no-redirect-rules is fulfilled.
          sql(s"update delta.`$source` set c0 = 100")

          // 2.3. DELETE should be aborted because no-redirect-rules only allow UPDATE.
          val exception3 = intercept[DeltaIllegalStateException] {
            sql(s"delete from delta.`$source` where c0 = 1")
          }
          assert(exception3.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 2.4. Disabling SKIP_REDIRECT_FEATURE should allow all DMLs to pass.
          withSQLConf(DeltaSQLConf.SKIP_REDIRECT_FEATURE.key -> "true") {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
            sql(s"delete from delta.`$source` where c0 = 1")
          }

          // 3. Move table redirect to RedirectReady state with no-redirect-rules that
          // allows Write on appName "etl" .
          noRedirectRules = Set(
            NoRedirectRule(
              appName = Some("etl"),
              allowedOperations = Set(DeltaOperations.Write(SaveMode.Append).name)
            )
          )
          feature.update(deltaLog, catalogTableOpt, RedirectReady, redirectSpec, noRedirectRules)
          validateState(deltaLog, RedirectReady, dest, feature)

          // 3.1. The WRITE of appName "dummy" would be aborted because no-redirect-rules
          //      only allow WRITE on application "etl".
          val exception4 = intercept[DeltaIllegalStateException] {
            spark.conf.set("spark.app.name", "dummy")
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
          }
          assert(exception4.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 3.1. WRITE should pass
          spark.conf.set("spark.app.name", "etl")
          sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")

          // 3.2. UPDATE should be aborted because no-redirect-rules only allow WRITE.
          val exception5 = intercept[DeltaIllegalStateException] {
            sql(s"update delta.`$source` set c0 = 100")
          }
          assert(exception5.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 3.3. DELETE should be aborted because no-redirect-rules only allow WRITE.
          val exception6 = intercept[DeltaIllegalStateException] {
            sql(s"delete from delta.`$source` where c0 = 1")
          }
          assert(exception6.getErrorClass == "DELTA_NO_REDIRECT_RULES_VIOLATED")

          // 3.4. Disabling SKIP_REDIRECT_FEATURE should allow all DMLs to pass.
          withSQLConf(DeltaSQLConf.SKIP_REDIRECT_FEATURE.key -> "true") {
            sql(s"insert into delta.`$source` values(1),(2),(3),(4),(5),(6)")
            sql(s"update delta.`$source` set c0 = 100")
            sql(s"delete from delta.`$source` where c0 = 1")
          }
      }
    }

    def alterRedirect(
        table: String,
        redirectType: String,
        redirectState: RedirectState,
        spec: RedirectSpec,
        noRedirectRules: Set[NoRedirectRule]
    ): Unit = {
      val enableConfig = TableRedirectConfiguration(
        redirectType,
        redirectState.name,
        JsonUtils.toJson(spec),
        noRedirectRules
      )
      val enableConfigJson = StringEscapeUtils.escapeJson(JsonUtils.toJson(enableConfig))
      sql(s"alter table $table set TBLPROPERTIES('$featureName' = '$enableConfigJson')")
    }

    redirectTest(s"Redirect $featureName: modify table property", accessSource = true) {
      case (_, source, dest, catalogTable) =>
        if (!TestMode.unityCatalogTestsEnabled) {
          val redirectSpec = new PathBasedRedirectSpec(dest.getCanonicalPath + "/_delta_log")
          val redirectType = PathBasedRedirectSpec.REDIRECT_TYPE
          val destPath = dest.toString
          val srcPath = source.toString
          sql(s"CREATE external TABLE t2(c0 long) USING delta LOCATION '$dest';")
          sql(s"insert into t2 values(1),(2),(3),(4),(5)")
          val destTable = s"delta.`$destPath`"
          val srcTable = s"delta.`$srcPath`"
          // Initialize the redirection by moving table into EnableRedirectInProgress state.
          alterRedirect(srcTable, redirectType, EnableRedirectInProgress, redirectSpec, Set.empty)
          alterRedirect(destTable, redirectType, EnableRedirectInProgress, redirectSpec, Set.empty)
          // Delta log is cloned, then moves both redirect destination table and redirect source
          // table to RedirectReady state.
          alterRedirect(srcTable, redirectType, RedirectReady, redirectSpec, Set.empty)
          alterRedirect(destTable, redirectType, RedirectReady, redirectSpec, Set.empty)
          sql(s"insert into $srcTable values(1), (2), (3)")
          sql(s"insert into $destTable values(1), (2), (3)")
          sql(s"insert into t1 values(1), (2), (3)")
          sql(s"insert into t2 values(1), (2), (3)")

          var result = sql("select * from t1").collect()
          assert(result.length == 17)
          result = sql("select * from t2").collect()
          assert(result.length == 17)
          result = sql(s"select * from $srcTable ").collect()
          assert(result.length == 17)
          result = sql(s"select * from $destTable ").collect()
          assert(result.length == 17)
          val root = new Path(catalogTable.location)
          val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
          var files = fs.listStatus(new Path(srcPath + "/_delta_log"))
            .filter(_.getPath.toString.endsWith(".json"))
          assert(files.length == 3)
          files = fs.listStatus(new Path(destPath + "/_delta_log"))
            .filter(_.getPath.toString.endsWith(".json"))
          assert(files.length == 9)
          // Drop redirection by moving both redirect destination table and redirect source table to
          // DropRedirectInProgress.
          alterRedirect(destTable, redirectType, DropRedirectInProgress, redirectSpec, Set.empty)
          alterRedirect(srcTable, redirectType, DropRedirectInProgress, redirectSpec, Set.empty)
          // Remove table redirect feature from redirect source table and verify table content.
          sql(s"alter table $srcTable unset TBLPROPERTIES('$featureName')")
          result = sql("select * from t1").collect()
          assert(result.length == 0)
          sql("insert into t1 values(1), (2), (3), (4)")
          result = sql("select * from t1").collect()
          assert(result.length == 4)
        }
    }
  }
}
