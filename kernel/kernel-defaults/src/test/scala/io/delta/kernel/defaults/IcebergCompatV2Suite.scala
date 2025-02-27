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
package io.delta.kernel.defaults

/** This suite tests reading or writing into Delta table that have `icebergCompatV2` enabled. */
class IcebergCompatV2Suite extends DeltaTableWriteSuiteBase {
  test("allowed data types") {}

  test("disallowed data types") {}

  test("allowed partition col types") {}

  test("disallowed partition col types") {}

  test("enabled icebergCompatV2 on a new table") {}

  test("enabling icebergCompatV2 on an existing table") {}

  test("can't be enabled on a table with deletion vectors supported") {}

  test("protocol has icebergCompatV2 and column mapping enabled") {}

  test("column mapping mode `name` is auto enabled when icebergCompatV2 is enabled") {}

  test("existing column mapping mode `name` is preserved when icebergCompatV2 is enabled") {}

  test("existing column mapping mode `id` is preserved when icebergCompatV2 is enabled") {}

  test("can't enable icebergCompatV2 on a table with type widening supported") {}

  test("can't enable icebergCompatV2 on a table with icebergCompatv1 enabled") {}
}
