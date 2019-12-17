/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.hive.test

import org.scalatest.Ignore

// TODO Figure out why running this test will cause other tests fail. Probably due to some unknonw
// dependency conflicts.
@Ignore
class HiveTestSuite extends HiveTest {

  test("basic hive query") {
    runQuery("create database testdb1")
    runQuery("use testdb1")
    runQuery("create table testtbl1 as select 'foo' as key")
    assert(runQuery("select * from testdb1.testtbl1") === Seq("foo"))
  }
}
