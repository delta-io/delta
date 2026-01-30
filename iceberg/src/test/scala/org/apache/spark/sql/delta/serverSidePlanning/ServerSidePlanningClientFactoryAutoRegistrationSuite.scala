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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for ServerSidePlanningClientFactory auto-registration with
 * IcebergRESTCatalogPlanningClientFactory.
 *
 * These tests verify that the ServiceLoader-based auto-registration mechanism
 * correctly discovers and registers the IcebergRESTCatalogPlanningClientFactory
 * when it's on the classpath.
 */
class ServerSidePlanningClientFactoryAutoRegistrationSuite
    extends QueryTest
    with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Execute test block with clean factory state (setup + teardown).
   */
  private def withCleanFactory[T](testFn: => T): T = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
      testFn
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("auto-registration succeeds when IcebergRESTCatalogPlanningClientFactory " +
    "is on classpath") {
    withCleanFactory {
      // Verify factory is not registered initially
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should not be registered initially")
      assert(ServerSidePlanningClientFactory.getRegisteredFactoryName().isEmpty,
        "Factory info should be empty initially")

      // Calling getFactory() should trigger auto-registration
      val factory = ServerSidePlanningClientFactory.getFactory()

      // Verify factory is successfully registered
      assert(factory != null, "Factory should not be null after auto-registration")
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after getFactory() call")

      // Verify it's the correct type
      val factoryInfo = ServerSidePlanningClientFactory.getRegisteredFactoryName()
      assert(factoryInfo.isDefined,
        "Factory info should be defined after auto-registration")
      assert(factoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Expected IcebergRESTCatalogPlanningClientFactory, got: ${factoryInfo.get}")
    }
  }

  test("autoRegistrationAttempted flag prevents multiple registration attempts") {
    withCleanFactory {
      // First call triggers auto-registration
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factoryInfo1 = ServerSidePlanningClientFactory.getRegisteredFactoryName()

      // Multiple calls should return the same cached instance
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      // Verify all calls return the same instance (reference equality)
      assert(factory1 eq factory2,
        "Second getFactory() call should return cached instance")
      assert(factory2 eq factory3,
        "Third getFactory() call should return cached instance")

      // Verify factory info remains consistent
      assert(ServerSidePlanningClientFactory.getRegisteredFactoryName() == factoryInfo1,
        "Factory info should remain consistent across multiple calls")

      // After clearFactory(), should allow fresh registration
      ServerSidePlanningClientFactory.clearFactory()
      val factory4 = ServerSidePlanningClientFactory.getFactory()

      // Verify new registration occurred
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after clearFactory() and getFactory()")
      assert(ServerSidePlanningClientFactory.getRegisteredFactoryName().isDefined,
        "Factory info should be defined after fresh registration")
      assert(ServerSidePlanningClientFactory.getRegisteredFactoryName().get
        .contains("IcebergRESTCatalogPlanningClientFactory"),
        "Fresh registration should register Iceberg factory")
    }
  }
}
