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

import org.apache.spark.sql.delta.AdaptiveMetadataTableFeature
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite

class AMTUtilsSuite extends SparkFunSuite {

  private val tableRoot = new Path("file:/tables/t1")
  private val fs = tableRoot.getFileSystem(new Configuration())

  test("hasScheme: follows URI scheme grammar") {
    assert(AMTUtils.hasScheme("s3://bucket/path"))
    assert(AMTUtils.hasScheme("file:/tmp/table"))
    assert(AMTUtils.hasScheme("git+ssh://host/repo"))
    assert(AMTUtils.hasScheme("ab.c-1+2://host/repo"))

    assert(!AMTUtils.hasScheme("data/partition=key:value/file.parquet"))
    assert(!AMTUtils.hasScheme("metadata/snap-123:456.avro"))
    assert(!AMTUtils.hasScheme("3com://host"))
    assert(!AMTUtils.hasScheme("+ssh://host"))
    assert(!AMTUtils.hasScheme(".bar://host"))
  }

  test("isAbsoluteLocation: detects absolute locations without constructing Path") {
    assert(AMTUtils.isAbsoluteLocation("s3://bucket/path"))
    assert(AMTUtils.isAbsoluteLocation("file:/tmp/table"))
    assert(AMTUtils.isAbsoluteLocation("git+ssh://host/repo"))
    assert(AMTUtils.isAbsoluteLocation("/tmp/table"))

    assert(!AMTUtils.isAbsoluteLocation("metadata/file.parquet"))
    assert(!AMTUtils.isAbsoluteLocation("data/partition=key:value/file.parquet"))
    assert(!AMTUtils.isAbsoluteLocation("metadata/snap-123:456.avro"))
    assert(!AMTUtils.isAbsoluteLocation("3com://host"))
  }

  test("relativizeLocation: child locations under the table location become relative") {
    val root = "s3://bucket/db/table"
    Seq(
      "s3://bucket/db/table/metadata/file.parquet" -> "metadata/file.parquet",
      "s3://bucket/db/table/data/00000-0.parquet" -> "data/00000-0.parquet"
    ).foreach { case (location, expected) =>
      assert(AMTUtils.relativizeLocation(root, location) === expected)
    }
  }

  test("relativizeLocation: non-child locations stay unchanged") {
    val root = "s3://bucket/db/table"
    Seq(
      "s3://other-bucket/db/table/data/file.parquet",
      "s3://bucket/db/other-table/data/file.parquet",
      "s3://bucket/db/table_v2/data/00000-0.parquet",
      root
    ).foreach { location =>
      assert(AMTUtils.relativizeLocation(root, location) === location)
    }
  }

  test("relativizeLocation: mismatched file URI forms stay unchanged") {
    assert(AMTUtils.relativizeLocation(
      "file:/tmp/table", "file:///tmp/table/metadata/file.parquet") ===
      "file:///tmp/table/metadata/file.parquet")
    assert(AMTUtils.relativizeLocation(
      "file:///tmp/table", "file:/tmp/table/metadata/file.parquet") ===
      "file:/tmp/table/metadata/file.parquet")
  }

  test("relativizeLocation: trailing slash table location matches child") {
    assert(AMTUtils.relativizeLocation(
      "s3://bucket/db/table/", "s3://bucket/db/table/data/00000-0.parquet") ===
      "data/00000-0.parquet")
  }

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

  test("amtEnabled: true when the protocol supports the AMT feature") {
    val protocol = Protocol.forTableFeature(AdaptiveMetadataTableFeature)
    assert(AMTUtils.amtEnabled(Metadata(), protocol))
  }

  test("amtEnabled: false when the protocol lacks the AMT feature") {
    assert(!AMTUtils.amtEnabled(Metadata(), Protocol()))
  }
}
