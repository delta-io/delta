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

package org.apache.spark.sql.delta.clustering

import org.apache.spark.sql.delta.skipping.clustering.ClusteringColumn

import org.apache.spark.SparkFunSuite

class ClusteringMetadataDomainSuite extends SparkFunSuite {
  test("serialized string follows the spec") {
    val clusteringColumns = Seq(ClusteringColumn(Seq("col1", "`col2,col3`", "`col4.col5`,col6")))
    val clusteringMetadataDomain = ClusteringMetadataDomain.fromClusteringColumns(clusteringColumns)
    val serializedString = clusteringMetadataDomain.toDomainMetadata.json
    assert(serializedString ===
      """|{"domainMetadata":{"domain":"delta.clustering","configuration":
         |"{\"clusteringColumns\":[[\"col1\",\"`col2,col3`\",\"`col4.col5`,col6\"]],
         |\"domainName\":\"delta.clustering\"}","removed":false}}""".stripMargin.replace("\n", ""))
  }
}
