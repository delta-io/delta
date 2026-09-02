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
package io.delta.spark.internal.v2.tablemanager

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaV2CacheKeySuite
    extends QueryTest
    with SharedSparkSession
{

  test("produces a _delta_log path from the data path") {
    withTempDir { dataPath =>
      val key = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath,
        Map.empty[String, String].asJava)

      assert(key.path.isAbsolute)
      assert(key.path.toString.endsWith("_delta_log"))
      val parentUri = key.path.getParent.toUri
      val expectedUri = new Path(
        dataPath.getCanonicalPath).toUri.normalize()
      assert(parentUri.normalize().getPath ===
        expectedUri.getPath,
        "qualified parent path component must match data path")
    }
  }

  test("same inputs produce equal keys") {
    withTempDir { dataPath =>
      val options = Map(
        "fs.test.option" -> "value", "reader.option" -> "ignored")
      val first = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath, options.asJava)
      val second = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath, options.asJava)
      assert(first === second)
    }
  }

  test("preserves path derivation for paths containing colons") {
    withTempDir { parent =>
      val dataPath = new File(parent, "table:with:colons")
      assert(dataPath.mkdirs())

      withSQLConf(
          DeltaSQLConf.DELTA_WORK_AROUND_COLONS_IN_HADOOP_PATHS.key -> "true") {
        val key = DeltaV2CacheKey.from(
          spark, dataPath.getCanonicalPath, Map.empty[String, String].asJava)
        assert(key.path.isAbsolute)
        assert(key.path.toString.endsWith("_delta_log"))
      }
    }
  }

  test("filters non-filesystem options and isolates filesystem credentials") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val firstOptions = Map(
        "fs.test.secret" -> "first",
        "dfs.test.endpoint" -> "endpoint",
        "reader.option" -> "ignored")
      val secondOptions = firstOptions.updated("fs.test.secret", "second")

      val first = DeltaV2CacheKey.from(spark, path, firstOptions.asJava)
      val second = DeltaV2CacheKey.from(spark, path, secondOptions.asJava)

      assert(first.sessionInvariantFsOptions === firstOptions.filter { case (key, _) =>
        key.startsWith("fs.") || key.startsWith("dfs.")
      })
      assert(!first.sessionInvariantFsOptions.contains("reader.option"))
      assert(first !== second)
    }
  }

  test("redacts filesystem option names and values from rendering") {
    withTempDir { dataPath =>
      val optionName = "fs.test.secret"
      val optionValue = "credential-value"
      val key = DeltaV2CacheKey.from(
        spark, dataPath.getCanonicalPath,
        Map(optionName -> optionValue).asJava)
      val rendered = key.toString

      assert(!rendered.contains(optionName))
      assert(!rendered.contains(optionValue))
      assert(rendered.contains("fsOptions=<redacted>"))
    }
  }

  // --- Local-filesystem path normalization -------------------------

  test("trailing slash in data path converges to same key") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val empty = Map.empty[String, String].asJava
      val withSlash = DeltaV2CacheKey.from(
        spark, path + "/", empty)
      val withoutSlash = DeltaV2CacheKey.from(
        spark, path, empty)
      assert(withSlash === withoutSlash)
    }
  }

  test("explicit file scheme alias converges to same key") {
    withTempDir { dataPath =>
      val plain = dataPath.getCanonicalPath
      val withScheme = "file://" + plain
      val empty = Map.empty[String, String].asJava
      val keyPlain = DeltaV2CacheKey.from(
        spark, plain, empty)
      val keyScheme = DeltaV2CacheKey.from(
        spark, withScheme, empty)
      assert(keyPlain === keyScheme)
    }
  }

  test("different fs options produce distinct keys for same path") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val keyA = DeltaV2CacheKey.from(
        spark, path, Map("fs.test" -> "a").asJava)
      val keyB = DeltaV2CacheKey.from(
        spark, path, Map("fs.test" -> "b").asJava)
      assert(keyA !== keyB)
    }
  }

  // --- LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS off ---------

  test("DataFrame fs options ignored when config is off") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val confKey =
        DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS.key
      withSQLConf(confKey -> "false") {
        val withFsOpts = DeltaV2CacheKey.from(spark, path,
          Map("fs.test" -> "v", "dfs.test" -> "x").asJava)
        val bare = DeltaV2CacheKey.from(
          spark, path, Map.empty[String, String].asJava)
        assert(withFsOpts === bare,
          "fs/dfs DataFrame options should be ignored")
        assert(withFsOpts.sessionInvariantFsOptions.isEmpty)
      }
    }
  }

  test("catalog storage properties authoritative when config off") {
    withTempDir { dataPath =>
      val path = dataPath.getCanonicalPath
      val confKey =
        DeltaSQLConf.LOAD_FILE_SYSTEM_CONFIGS_FROM_DATAFRAME_OPTIONS.key
      val catalogTable = CatalogTable(
        identifier = TableIdentifier("test_table"),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty.copy(
          properties =
            Map("fs.catalog.key" -> "catalog-value")),
        schema = new StructType())
      withSQLConf(confKey -> "false") {
        val withCatalog = DeltaV2CacheKey.from(
          spark, path,
          Map("fs.ignored" -> "from-options").asJava,
          Some(catalogTable))
        val bare = DeltaV2CacheKey.from(
          spark, path, Map.empty[String, String].asJava)
        assert(withCatalog !== bare,
          "catalog fs options should differentiate keys")
        assert(withCatalog.sessionInvariantFsOptions
          .contains("fs.catalog.key"))
        assert(!withCatalog.sessionInvariantFsOptions
          .contains("fs.ignored"))
      }
    }
  }

  // --- Qualification and non-retention invariants ------------------

  test("absolute schemeless path is qualified with a URI scheme") {
    withTempDir { dataPath =>
      val schemelessAbsolute = dataPath.getCanonicalPath
      assert(!schemelessAbsolute.contains("://"),
        "precondition: input must not carry a URI scheme")
      val key = DeltaV2CacheKey.from(
        spark, schemelessAbsolute,
        Map.empty[String, String].asJava)

      // Independently compute the expected qualified path using
      // the same Hadoop configuration and FileSystem that the
      // production code would use.
      val rawLogPath =
        new Path(new Path(schemelessAbsolute), "_delta_log")
      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState
        .newHadoopConfWithOptions(Map.empty)
      // scalastyle:on deltahadoopconfiguration
      val expectedPath =
        rawLogPath.getFileSystem(hadoopConf)
          .makeQualified(rawLogPath)

      assert(key.path === expectedPath,
        "key path must equal the FileSystem-qualified path")
      assert(key.path.toUri.getScheme != null,
        "qualified path must carry a URI scheme")
      assert(key.path.toUri.getAuthority != null ||
        key.path.toUri.getScheme == "file",
        "qualified path must carry authority or be local")
    }
  }

  test("cache key does not retain a Hadoop FileSystem instance") {
    // Deterministic structural assertion: none of the case-class
    // fields have a type assignable to Hadoop FileSystem. This pins
    // the non-retention invariant without relying on GC timing or
    // weak references, which are invalid under Hadoop's global
    // FileSystem cache.
    val fsClass = classOf[FileSystem]
    val fields = classOf[DeltaV2CacheKey].getDeclaredFields
    val fsFields = fields.filter { f =>
      fsClass.isAssignableFrom(f.getType)
    }
    assert(fsFields.isEmpty,
      s"DeltaV2CacheKey must not hold a FileSystem; found: " +
        fsFields.map(_.getName).mkString(", "))
  }
}
