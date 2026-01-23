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

package io.delta.internal

import java.net.URI
import java.util.{HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.delta.Relocated.StreamingRelation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ApplyV2StreamingSuite extends DeltaSQLCommandTest {

  private def applyRule(plan: LogicalPlan): LogicalPlan = {
    new ApplyV2Streaming(spark).apply(plan)
  }

  private def assertV2(result: LogicalPlan): Unit = {
    result match {
      case StreamingRelationV2(_, _, _: SparkTable, _, _, _, _, v1Relation) =>
        assert(v1Relation.isEmpty)
      case other =>
        fail(s"Expected StreamingRelationV2, got $other")
    }
  }

  private def assertV1(result: LogicalPlan): Unit = {
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

  private val relationBuilders: Seq[(String, (CatalogTable, String) => LogicalPlan)] = Seq(
    ("streaming", (catalogTable, path) => {
      val dataSource = DataSource(
        sparkSession = spark,
        userSpecifiedSchema = None,
        className = "delta",
        options = Map("path" -> path),
        catalogTable = Some(catalogTable))
      StreamingRelation(dataSource)
    }),
    ("non-streaming", (catalogTable, _) => {
      val ident = Identifier.of(
        catalogTable.identifier.database.toArray,
        catalogTable.identifier.table)
      val table = new SparkTable(ident, catalogTable, new JHashMap[String, String]())
      DataSourceV2Relation.create(
        table,
        None,
        None,
        CaseInsensitiveStringMap.empty)
    })
  )

  private val modes: Seq[(String, Boolean)] = Seq(
    ("STRICT", true),
    ("AUTO", false),
    ("NONE", false)
  )

  modes.foreach { case (mode, expectV2) =>
    relationBuilders.foreach { case (relationType, buildPlan) =>
      test(s"ApplyV2Streaming respects $mode mode for $relationType relation") {
        withTempDir { dir =>
          val path = dir.getCanonicalPath
          createDeltaTable(path)
          val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
          val plan = buildPlan(catalogTable, path)

          withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> mode) {
            val result = applyRule(plan)
            if (relationType == "streaming") {
              if (expectV2) {
                assertV2(result)
              } else {
                assertV1(result)
              }
            } else {
              assert(result == plan)
            }
          }
        }
      }
    }
  }
}
