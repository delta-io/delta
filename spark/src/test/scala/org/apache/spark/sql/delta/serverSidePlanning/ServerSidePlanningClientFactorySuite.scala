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
 * Unit tests for ServerSidePlanningClientFactory auto-registration and lifecycle.
 * Tests the reflection-based auto-registration with double-checked locking,
 * manual factory registration, and factory lifecycle management.
 */
class ServerSidePlanningClientFactorySuite extends QueryTest with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  test("auto-registration succeeds when IcebergRESTCatalogPlanningClientFactory " +
    "is on classpath") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // Verify factory is not registered initially
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should not be registered initially")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "Factory info should be empty initially")

      // Calling getFactory() should trigger auto-registration
      val factory = ServerSidePlanningClientFactory.getFactory()

      // Verify factory is successfully registered
      assert(factory != null, "Factory should not be null after auto-registration")
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after getFactory() call")

      // Verify it's the correct type
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined,
        "Factory info should be defined after auto-registration")
      assert(factoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Expected IcebergRESTCatalogPlanningClientFactory, got: ${factoryInfo.get}")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("getFactory returns same instance across multiple calls") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // First call triggers auto-registration
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      // Verify all calls return the same instance (reference equality)
      assert(factory1 eq factory2,
        "Second getFactory() call should return same instance as first")
      assert(factory2 eq factory3,
        "Third getFactory() call should return same instance as second")

      // Verify factory info remains consistent
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined,
        "Factory info should be defined after multiple getFactory() calls")
      assert(factoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Factory should remain IcebergRESTCatalogPlanningClientFactory, got: ${factoryInfo.get}")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("manual setFactory overrides auto-registration") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // Manually set a test factory BEFORE auto-registration
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Verify factory is registered
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after setFactory()")

      // getFactory() should return the manually-set factory
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq testFactory,
        "getFactory() should return the manually-set factory")

      // Verify factory info reflects the manual factory
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined, "Factory info should be defined")
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Expected TestServerSidePlanningClientFactory, got: ${factoryInfo.get}")

      // Auto-registration should not be triggered
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        "Auto-registration should not have occurred")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("manual setFactory after auto-registration replaces factory") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // Trigger auto-registration first
      val autoFactory = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.getFactoryInfo().get
        .contains("IcebergRESTCatalogPlanningClientFactory"),
        "Should have auto-registered Iceberg factory")

      // Now manually set a test factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)

      // Subsequent getFactory() calls should return the manual factory
      val retrievedFactory = ServerSidePlanningClientFactory.getFactory()
      assert(retrievedFactory eq testFactory,
        "getFactory() should return the manually-set factory after replacement")

      // Verify factory info reflects the new factory
      val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(factoryInfo.isDefined, "Factory info should be defined")
      assert(factoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Expected TestServerSidePlanningClientFactory after replacement, got: ${factoryInfo.get}")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("clearFactory resets both registeredFactory and autoRegistrationAttempted flag") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // First registration cycle
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in first cycle")
      val factoryInfo1 = ServerSidePlanningClientFactory.getFactoryInfo()

      // Clear factory
      ServerSidePlanningClientFactory.clearFactory()

      // Verify factory is no longer registered
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should not be registered after clearFactory()")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "Factory info should be empty after clearFactory()")

      // Second registration cycle - should trigger fresh auto-registration
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in second cycle")
      val factoryInfo2 = ServerSidePlanningClientFactory.getFactoryInfo()

      // Verify both cycles registered the same type of factory
      assert(factoryInfo1 == factoryInfo2,
        "Both registration cycles should register the same factory type")

      // Third cycle to ensure it continues to work
      ServerSidePlanningClientFactory.clearFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered in third cycle")

      // Verify multiple registration cycles work correctly
      assert(ServerSidePlanningClientFactory.getFactoryInfo() == factoryInfo1,
        "All cycles should register the same factory type")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("isFactoryRegistered correctly reports registration state") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // Initially not registered
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Should not be registered initially")

      // After manual registration
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Should be registered after setFactory()")

      // After clearFactory
      ServerSidePlanningClientFactory.clearFactory()
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Should not be registered after clearFactory()")

      // After auto-registration
      ServerSidePlanningClientFactory.getFactory()
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Should be registered after auto-registration via getFactory()")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("getFactoryInfo returns correct factory class information") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // Returns None when no factory registered
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "getFactoryInfo() should return None when no factory is registered")

      // Returns correct class name for test factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      val testFactoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(testFactoryInfo.isDefined,
        "getFactoryInfo() should return Some when factory is registered")
      assert(testFactoryInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Expected TestServerSidePlanningClientFactory, got: ${testFactoryInfo.get}")

      // Returns correct class name for auto-registered factory
      ServerSidePlanningClientFactory.clearFactory()
      ServerSidePlanningClientFactory.getFactory()
      val autoFactoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(autoFactoryInfo.isDefined,
        "getFactoryInfo() should return Some after auto-registration")
      assert(autoFactoryInfo.get.contains("IcebergRESTCatalogPlanningClientFactory"),
        s"Expected IcebergRESTCatalogPlanningClientFactory, got: ${autoFactoryInfo.get}")

      // Updates correctly when factory changes
      ServerSidePlanningClientFactory.setFactory(testFactory)
      val updatedInfo = ServerSidePlanningClientFactory.getFactoryInfo()
      assert(updatedInfo.isDefined,
        "getFactoryInfo() should return Some after factory change")
      assert(updatedInfo.get.contains("TestServerSidePlanningClientFactory"),
        s"Factory info should update after setFactory(), got: ${updatedInfo.get}")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("autoRegistrationAttempted flag prevents multiple registration attempts") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // First call triggers auto-registration
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factoryInfo1 = ServerSidePlanningClientFactory.getFactoryInfo()

      // Multiple calls should return the same cached instance
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      // Verify all calls return the same instance (reference equality)
      assert(factory1 eq factory2,
        "Second getFactory() call should return cached instance")
      assert(factory2 eq factory3,
        "Third getFactory() call should return cached instance")

      // Verify factory info remains consistent
      assert(ServerSidePlanningClientFactory.getFactoryInfo() == factoryInfo1,
        "Factory info should remain consistent across multiple calls")

      // After clearFactory(), should allow fresh registration
      ServerSidePlanningClientFactory.clearFactory()
      val factory4 = ServerSidePlanningClientFactory.getFactory()

      // Verify new registration occurred
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered after clearFactory() and getFactory()")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isDefined,
        "Factory info should be defined after fresh registration")
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  test("verify test isolation - factory state doesn't leak between tests") {
    try {
      ServerSidePlanningClientFactory.clearFactory()

      // This test verifies that afterEach() cleanup works correctly
      // and tests start with clean state

      // Verify starting with clean state
      assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Test should start with no factory registered")
      assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
        "Test should start with empty factory info")

      // Set a factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
        "Factory should be registered within test")

      // afterEach() will clean this up automatically
      // The next test should start fresh
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }
}
