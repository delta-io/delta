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

package org.apache.spark.sql.delta.amt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite

class AMTUtilsSuite extends SparkFunSuite {

  private val tableRoot = new Path("file:/tables/t1")
  private val fs = tableRoot.getFileSystem(new Configuration())

  test("relativizeManifestPathToTableRoot: a file under the table root becomes relative") {
    val leaf = new Path("file:/tables/t1/metadata/leaf-1.parquet")
    assert(AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, leaf) ===
      "metadata/leaf-1.parquet")
  }

  test("relativizeManifestPathToTableRoot: a file outside the table root stays absolute") {
    val outside = new Path("file:/other/metadata/leaf-1.parquet")
    assert(AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, outside) ===
      "file:/other/metadata/leaf-1.parquet")
  }

  test("relativizeManifestPathToTableRoot: result is raw, not URL-encoded") {
    val leaf = new Path("file:/tables/t1/metadata/leaf a.parquet")
    val relative = AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, leaf)
    assert(relative === "metadata/leaf a.parquet",
      s"space must stay raw, not percent-encoded; got $relative")
    assert(!relative.contains("%20"))
  }

  test("absolutePathForManifestFile: a relative location joins raw onto the table root") {
    val resolved = AMTUtils.absolutePathForManifestFile(
      new Path("s3://bucket/tables/t1"), "metadata/leaf a.parquet")
    assert(resolved === new Path("s3://bucket/tables/t1/metadata/leaf a.parquet"))
    assert(!resolved.toString.contains("%20"),
      s"relative location must resolve raw, not URL-encoded; got $resolved")
  }

  test("absolutePathForManifestFile: a location with a URI scheme is used as-is") {
    val resolved = AMTUtils.absolutePathForManifestFile(
      new Path("s3://bucket/tables/t1"), "s3://other/manifests/root x.parquet")
    assert(resolved === new Path("s3://other/manifests/root x.parquet"))
  }

  test("relativize then absolutize round-trips a file under the table root") {
    val root = new Path("file:/tables/t1")
    val leaf = new Path("file:/tables/t1/metadata/leaf-1.parquet")
    val relative = AMTUtils.relativizeManifestPathToTableRoot(fs, root, leaf)
    assert(AMTUtils.absolutePathForManifestFile(root, relative) === leaf)
  }
}
