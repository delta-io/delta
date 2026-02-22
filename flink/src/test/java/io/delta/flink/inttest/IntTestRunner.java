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

package io.delta.flink.inttest;

import com.databricks.connect.DatabricksSession;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.URI;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/**
 * Pure Java 17 IntTest runner (no new deps).
 *
 * <p>Features: - Runs ONLY the explicitly listed test classes in TEST_CASES. - Each test class must
 * extend IntTestBase. - Discovers instance methods annotated with @IntTest (0 args). - Honors
 * lifecycle methods annotated with: - org.junit.jupiter.api.BeforeEach -
 * org.junit.jupiter.api.AfterEach (Also supports JUnit4 @Before/@After if present, without
 * depending on it.) - Does NOT support BeforeAll/AfterAll. - Creates a NEW instance per test method
 * (JUnit-style), so @BeforeEach/@AfterEach run per test.
 *
 * <p>Assumptions: - @IntTest is RUNTIME-retained. - Your IntTest classes have an accessible no-arg
 * constructor (or at least reflectively accessible).
 *
 * <p>Exit code: - 0 if all tests pass - 1 if any fail - 2 if required base/annotation types missing
 */
public final class IntTestRunner {

  // -----------------------
  // Configure test classes here
  // -----------------------
  static final List<Class<? extends IntTestBase>> TEST_CASES =
      List.of(UnityCatalogIntTest.class, CatalogManagedTableIntTest.class);

  private static final ClassLoader LOADER = Thread.currentThread().getContextClassLoader();

  public static void main(String[] args) {
    ConfigParser.TestConfig config = ConfigParser.fromArgs(args);

    SparkSession sparkSession = connectToSpark(config);

    RunSummary summary = new RunSummary();
    long startMs = System.currentTimeMillis();

    // Resolve annotations by name to avoid compile-time dependency on JUnit.
    // If JUnit is present at runtime, we'll honor these annotations.
    Class<? extends Annotation> intTestAnn;
    Class<? extends Annotation> beforeEachAnn;
    Class<? extends Annotation> afterEachAnn;

    try {
      intTestAnn = loadAnnotationOrThrow("io.delta.flink.inttest.IntTest");
    } catch (Throwable t) {
      System.err.println(
          "Missing @IntTest annotation on classpath (or not runtime-retained): " + t);
      System.exit(2);
      return;
    }

    beforeEachAnn = loadAnnotationOrNull("org.junit.jupiter.api.BeforeEach");
    afterEachAnn = loadAnnotationOrNull("org.junit.jupiter.api.AfterEach");

    if (TEST_CASES.isEmpty()) {
      System.out.println("No test classes listed in TEST_CASES; nothing to run.");
      System.exit(0);
      return;
    }

    for (Class<? extends IntTestBase> testClass : TEST_CASES) {
      if (testClass == null) continue;

      if (!isConcreteSubclass(testClass, IntTestBase.class)) {
        summary.failedClasses++;
        summary.failures.add(
            new Failure(
                testClass.getName(),
                "<class>",
                new IllegalArgumentException(
                    "Class does not extend IntTestBase or is abstract/interface")));
        continue;
      }

      // Collect lifecycle hooks and tests
      List<Method> beforeHooks = new ArrayList<>();
      List<Method> afterHooks = new ArrayList<>();
      if (beforeEachAnn != null)
        beforeHooks.addAll(findAnnotatedNoArgInstanceMethods(testClass, beforeEachAnn));
      if (afterEachAnn != null)
        afterHooks.addAll(findAnnotatedNoArgInstanceMethods(testClass, afterEachAnn));

      // Deterministic ordering (JUnit doesn't guarantee, but deterministic is useful)
      beforeHooks.sort(Comparator.comparing(Method::getName));
      afterHooks.sort(Comparator.comparing(Method::getName));

      List<Method> tests = findAnnotatedNoArgInstanceMethods(testClass, intTestAnn);
      if (tests.isEmpty()) {
        summary.matchedClassesNoTests++;
        continue;
      }
      tests.sort(Comparator.comparing(Method::getName));
      summary.matchedClassesWithTests++;

      for (Method testMethod : tests) {
        String testId = testClass.getName() + "#" + testMethod.getName();
        Object instance;

        try {
          instance = instantiate(testClass, sparkSession, config.workspace, config.token);
        } catch (Throwable t) {
          summary.failedClasses++;
          summary.failures.add(new Failure(testClass.getName(), "<init>", t));
          System.out.println("[FAIL] " + testId + " -> failed to instantiate: " + rootMessage(t));
          continue; // can't run methods
        }

        try {
          // @BeforeEach / @Before
          for (Method m : beforeHooks) invoke(instance, m);

          // test
          invoke(instance, testMethod);

          // @AfterEach / @After
          // Always try to run after hooks even if test passed; if after fails, test fails.
          for (Method m : afterHooks) invoke(instance, m);

          summary.passed++;
          System.out.println("[PASS] " + testId);
        } catch (Throwable t) {
          // If test or before fails, still attempt after hooks (best effort).
          Throwable primary = t;
          Throwable afterErr = null;

          try {
            for (Method m : afterHooks) invoke(instance, m);
          } catch (Throwable t2) {
            afterErr = t2;
          }

          summary.failed++;
          if (afterErr != null) {
            primary.addSuppressed(afterErr);
          }
          summary.failures.add(new Failure(testClass.getName(), testMethod.getName(), primary));
          System.out.println("[FAIL] " + testId + " -> " + rootMessage(primary));
        }
      }
    }

    long elapsed = System.currentTimeMillis() - startMs;
    printSummary(summary, elapsed);

    System.exit((summary.failed > 0 || summary.failedClasses > 0) ? 1 : 0);
  }

  private static SparkSession connectToSpark(ConfigParser.TestConfig config) {
    return DatabricksSession.builder()
        .host(config.workspace.toString())
        .token(config.token)
        .clusterId(config.clusterId)
        .getOrCreate();
  }

  // ----------------------------
  // Reflection helpers
  // ----------------------------

  @SuppressWarnings("unchecked")
  private static Class<? extends Annotation> loadAnnotationOrThrow(String fqcnOrSimple)
      throws ClassNotFoundException {
    Class<?> c = Class.forName(fqcnOrSimple, false, LOADER);
    if (!Annotation.class.isAssignableFrom(c)) {
      throw new IllegalStateException(fqcnOrSimple + " is not an annotation type");
    }
    return (Class<? extends Annotation>) c;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends Annotation> loadAnnotationOrNull(String fqcn) {
    try {
      Class<?> c = Class.forName(fqcn, false, LOADER);
      if (!Annotation.class.isAssignableFrom(c)) return null;
      return (Class<? extends Annotation>) c;
    } catch (Throwable t) {
      return null; // JUnit not on classpath or annotation missing
    }
  }

  private static boolean isConcreteSubclass(Class<?> c, Class<?> base) {
    if (c == null) return false;
    int mod = c.getModifiers();
    if (Modifier.isInterface(mod) || Modifier.isAbstract(mod)) return false;
    return base.isAssignableFrom(c);
  }

  private static Object instantiate(Class<?> c, SparkSession spark, URI workspace, String token)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    Constructor<?> ctor = c.getDeclaredConstructor(SparkSession.class, URI.class, String.class);
    if (!ctor.canAccess(null)) ctor.setAccessible(true);
    return ctor.newInstance(spark, workspace, token);
  }

  private static List<Method> findAnnotatedNoArgInstanceMethods(
      Class<?> c, Class<? extends Annotation> ann) {
    List<Method> out = new ArrayList<>();

    // Include inherited methods as well (walk up to Object)
    for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
      for (Method m : k.getDeclaredMethods()) {
        if (Modifier.isStatic(m.getModifiers())) continue;
        if (m.getParameterCount() != 0) continue;
        if (m.getAnnotation(ann) == null) continue;
        m.setAccessible(true);
        out.add(m);
      }
    }
    return out;
  }

  private static void invoke(Object instance, Method m) throws Throwable {
    try {
      m.invoke(instance);
    } catch (InvocationTargetException ite) {
      throw ite.getTargetException();
    }
  }

  // ----------------------------
  // Reporting
  // ----------------------------

  private static final class Failure {
    final String className;
    final String methodName;
    final Throwable error;

    Failure(String className, String methodName, Throwable error) {
      this.className = className;
      this.methodName = methodName;
      this.error = error;
    }
  }

  private static final class RunSummary {
    int passed = 0;
    int failed = 0;
    int failedClasses = 0;
    int matchedClassesWithTests = 0;
    int matchedClassesNoTests = 0;
    final List<Failure> failures = new ArrayList<>();
  }

  private static void printSummary(RunSummary s, long ms) {
    System.out.println();
    System.out.println("==== IntTest Summary ====");
    System.out.println("Time: " + ms + " ms");
    System.out.println("Test classes listed: " + TEST_CASES.size());
    System.out.println("Matched classes w/ tests: " + s.matchedClassesWithTests);
    System.out.println("Matched classes no tests: " + s.matchedClassesNoTests);
    System.out.println("Passed tests: " + s.passed);
    System.out.println("Failed tests: " + s.failed);
    System.out.println("Failed class instantiations/validation: " + s.failedClasses);

    if (!s.failures.isEmpty()) {
      System.out.println();
      System.out.println("Failures:");
      for (Failure f : s.failures) {
        System.out.println(" - " + f.className + "#" + f.methodName + ": " + rootMessage(f.error));
        for (Throwable sup : f.error.getSuppressed()) {
          System.out.println("   suppressed: " + rootMessage(sup));
        }
      }
    }
  }

  private static String rootMessage(Throwable t) {
    Throwable r = t;
    while (r.getCause() != null && r.getCause() != r) r = r.getCause();
    String msg = r.getMessage();
    return r.getClass().getName() + (msg == null ? "" : (": " + msg));
  }
}
