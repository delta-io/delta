/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.sql

import java.net.URI
import java.util.{HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.delta.Relocated.StreamingRelation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType

class ApplyV2StreamingSuite extends DeltaSQLCommandTest {

  private def applyRule(plan: StreamingRelation): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    new ApplyV2Streaming(spark).apply(plan)
  }

  private def assertV2(result: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit = {
    result match {
      case StreamingRelationV2(_, _, _: SparkTable, _, _, _, _, v1Relation) =>
        assert(v1Relation.isEmpty)
      case other =>
        fail(s"Expected StreamingRelationV2, got $other")
    }
  }

  private def assertV1(result: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit = {
    assert(result.isInstanceOf[StreamingRelation])
    assert(!result.isInstanceOf[StreamingRelationV2])
  }

  private def createDeltaTable(path: String): Unit = {
    spark.range(1).selectExpr("id").write.format("delta").save(path)
  }

  private def createCatalogTable(locationUri: URI, ucManaged: Boolean): CatalogTable = {
    val storageProps = new JHashMap[String, String]()
    if (ucManaged) {
      storageProps.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, "uc-table-id")
      storageProps.put("delta.feature.catalogManaged", "supported")
    }
    val identifier = TableIdentifier("tbl", Some("default"), Some("spark_catalog"))
    val storage = CatalogStorageFormat(
      locationUri = Some(locationUri),
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = storageProps.asScala.toMap)
    CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = Map.empty)
  }

  private def makeStreamingRelation(
      catalogTable: CatalogTable,
      path: String): StreamingRelation = {
    val dataSource = DataSource(
      sparkSession = spark,
      userSpecifiedSchema = None,
      className = "delta",
      options = Map("path" -> path),
      catalogTable = Some(catalogTable))
    StreamingRelation(dataSource)
  }

  test("ApplyV2Streaming rewrites to StreamingRelationV2 in STRICT mode") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        createDeltaTable(dir.getCanonicalPath)
        val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
        val plan = makeStreamingRelation(catalogTable, dir.getCanonicalPath)
        val result = applyRule(plan)
        assertV2(result)
      }
    }
  }

  test("ApplyV2Streaming respects AUTO mode for non-UC tables") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "AUTO") {
        createDeltaTable(path)
        val nonUcTable = createCatalogTable(dir.toURI, ucManaged = false)
        val nonUcPlan = makeStreamingRelation(nonUcTable, path)
        val nonUcResult = applyRule(nonUcPlan)
        assertV1(nonUcResult)
      }
    }
  }

  test("ApplyV2Streaming does not rewrite in NONE mode") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        createDeltaTable(path)
        val nonUcTable = createCatalogTable(dir.toURI, ucManaged = false)
        val nonUcPlan = makeStreamingRelation(nonUcTable, path)
        val nonUcResult = applyRule(nonUcPlan)
        assertV1(nonUcResult)
      }
    }
  }
}
