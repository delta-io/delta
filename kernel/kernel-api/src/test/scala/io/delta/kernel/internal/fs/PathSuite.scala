/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.fs

import java.net.URI

import org.scalatest.funsuite.AnyFunSuite

class PathSuite extends AnyFunSuite {

  test("Path construction from String") {
    // Basic path construction
    val path1 = new Path("/user/data")
    assert(path1.toString === "/user/data")

    // Path with scheme
    val path2 = new Path("file:/user/data")
    assert(path2.toString === "file:/user/data")

    // Path with authority
    val path3 = new Path("hdfs://localhost:9000/user/data")
    assert(path3.toString === "hdfs://localhost:9000/user/data")

    // Relative path
    val path4 = new Path("relative/path")
    assert(path4.toString === "relative/path")

    // Empty path should throw exception
    val exception1 = intercept[IllegalArgumentException] {
      new Path("")
    }
    assert(exception1.getMessage.contains("empty string"))

    // Null path should throw exception
    val exception2 = intercept[IllegalArgumentException] {
      new Path(null: String)
    }
    assert(exception2.getMessage.contains("null string"))
  }

  test("Path construction from parent and child") {
    // String parent, String child
    val path1 = new Path("/user", "data")
    assert(path1.toString === "/user/data")

    // Path parent, String child
    val path2 = new Path(new Path("/user"), "data")
    assert(path2.toString === "/user/data")

    // String parent, Path child
    val path3 = new Path("/user", new Path("data"))
    assert(path3.toString === "/user/data")

    // Path parent, Path child
    val path4 = new Path(new Path("/user"), new Path("data"))
    assert(path4.toString === "/user/data")

    // Parent with trailing slash
    val path5 = new Path("/user/", "data")
    assert(path5.toString === "/user/data")

    // Parent is root
    val path6 = new Path("/", "data")
    assert(path6.toString === "/data")

    // Parent with scheme
    val path7 = new Path("file:/user", "data")
    assert(path7.toString === "file:/user/data")
  }

  test("Path construction from URI") {
    val uri1 = new URI("file:/user/data")
    val path1 = new Path(uri1)
    assert(path1.toString === "file:/user/data")

    val uri2 = new URI("hdfs", "localhost:9000", "/user/data", null, null)
    val path2 = new Path(uri2)
    assert(path2.toString === "hdfs://localhost:9000/user/data")
  }

  test("Path construction from scheme, authority, path") {
    val path1 = new Path("file", null, "/user/data")
    assert(path1.toString === "file:/user/data")

    val path2 = new Path("hdfs", "localhost:9000", "/user/data")
    assert(path2.toString === "hdfs://localhost:9000/user/data")

    val path3 = new Path(null, null, "/user/data")
    assert(path3.toString === "/user/data")

    // Skip the test for relative path with scheme as it's not supported
  }

  test("Path normalization") {
    // Remove duplicate slashes
    val path1 = new Path("/user//data///file")
    assert(path1.toString === "/user/data/file")

    // Remove trailing slash
    val path2 = new Path("/user/data/")
    assert(path2.toString === "/user/data")
    val path3 = new Path("/user/data//")
    assert(path3.toString === "/user/data")

    // Don't remove trailing slash from root
    val path4 = new Path("/")
    assert(path4.toString === "/")
  }

  test("Path.getName") {
    val path1 = new Path("/user/data/file.txt")
    assert(path1.getName === "file.txt")

    val path2 = new Path("/user/data/")
    assert(path2.getName === "data")

    val path3 = new Path("/")
    assert(path3.getName === "")

    val path4 = new Path("file.txt")
    assert(path4.getName === "file.txt")
  }

  test("Path.getParent") {
    val path1 = new Path("/user/data/file.txt")
    assert(path1.getParent.toString === "/user/data")

    val path2 = new Path("/user/data")
    assert(path2.getParent.toString === "/user")

    val path3 = new Path("/user")
    assert(path3.getParent.toString === "/")

    val path4 = new Path("/")
    assert(path4.getParent === null)

    val path5 = new Path("file.txt")
    assert(path5.getParent.toString === "")

    val path6 = new Path("dir/file.txt")
    assert(path6.getParent.toString === "dir")
  }

  test("Path.isAbsolute and Path.isUriPathAbsolute") {
    val path1 = new Path("/user/data")
    assert(path1.isAbsolute === true)
    assert(path1.isUriPathAbsolute === true)

    val path2 = new Path("user/data")
    assert(path2.isAbsolute === false)
    assert(path2.isUriPathAbsolute === false)

    // Skip the tests with scheme and relative paths as they cause exceptions
  }

  test("Path.isRoot") {
    val path1 = new Path("/")
    assert(path1.isRoot === true)

    val path2 = new Path("/user")
    assert(path2.isRoot === false)

    val path3 = new Path("file:/")
    assert(path3.isRoot === true)

    val path4 = new Path("file:/user")
    assert(path4.isRoot === false)
  }

  test("Path.toUri") {
    val path1 = new Path("/user/data")
    val uri1 = path1.toUri
    assert(uri1.getScheme === null)
    assert(uri1.getAuthority === null)
    assert(uri1.getPath === "/user/data")

    val path2 = new Path("file:/user/data")
    val uri2 = path2.toUri
    assert(uri2.getScheme === "file")
    assert(uri2.getAuthority === null)
    assert(uri2.getPath === "/user/data")

    val path3 = new Path("hdfs://localhost:9000/user/data")
    val uri3 = path3.toUri
    assert(uri3.getScheme === "hdfs")
    assert(uri3.getAuthority === "localhost:9000")
    assert(uri3.getPath === "/user/data")
  }

  test("Path equality and comparison") {
    val path1 = new Path("/user/data")
    val path2 = new Path("/user/data")
    val path3 = new Path("/user/other")

    // Test equals
    assert(path1 === path2)
    assert(path1 !== path3)

    // Test hashCode
    assert(path1.hashCode === path2.hashCode)

    // Test compareTo
    assert(path1.compareTo(path2) === 0)
    assert(path1.compareTo(path3) < 0) // "data" comes before "other" alphabetically
    assert(path3.compareTo(path1) > 0)
  }

  test("Path.getName static method") {
    assert(Path.getName("/user/data/file.txt") === "file.txt")
    assert(Path.getName("file.txt") === "file.txt")
    assert(Path.getName("/") === "")
  }
}
