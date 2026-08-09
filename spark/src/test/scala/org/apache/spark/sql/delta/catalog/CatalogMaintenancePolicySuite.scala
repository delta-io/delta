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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.SparkFunSuite

class CatalogMaintenancePolicySuite extends SparkFunSuite {

  test("filesystem tables retain existing maintenance behavior") {
    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = false,
      additionalOperations = Set.empty,
      operation = CatalogMaintenanceOperation.Optimize))
    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = false,
      additionalOperations = Set.empty,
      operation = CatalogMaintenanceOperation.Vacuum))
  }

  test("catalog-managed tables require the matching advertised operation") {
    val optimizeOnly = Set[CatalogMaintenanceOperation](CatalogMaintenanceOperation.Optimize)
    val vacuumOnly = Set[CatalogMaintenanceOperation](CatalogMaintenanceOperation.Vacuum)
    val optimizeAndVacuum = optimizeOnly ++ vacuumOnly

    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = optimizeOnly,
      operation = CatalogMaintenanceOperation.Optimize))
    assert(!CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = optimizeOnly,
      operation = CatalogMaintenanceOperation.Vacuum))
    assert(!CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = Set.empty,
      operation = CatalogMaintenanceOperation.Optimize))
    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = vacuumOnly,
      operation = CatalogMaintenanceOperation.Vacuum))
    assert(!CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = vacuumOnly,
      operation = CatalogMaintenanceOperation.Optimize))
    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = optimizeAndVacuum,
      operation = CatalogMaintenanceOperation.Optimize))
    assert(CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations = optimizeAndVacuum,
      operation = CatalogMaintenanceOperation.Vacuum))
  }

  test("REORG remains denied even if a caller tries to add it to the policy") {
    assert(!CatalogMaintenancePolicy.isAllowed(
      isCatalogOwned = true,
      additionalOperations =
        Set(CatalogMaintenanceOperation.Optimize, CatalogMaintenanceOperation.Reorg),
      operation = CatalogMaintenanceOperation.Reorg))
  }
}
