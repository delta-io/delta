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

package io.delta.spark.internal.v2;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

/**
 * Matrix test for DSv2 streaming reads over Delta tables in different configurations.
 *
 * <p>This is the skeleton wiring class. It defines {@link Setup} and {@link Mutation} enums and
 * connects them to the verification strategies in {@link V2StreamingMatrixTestBase}. New table
 * creation scenarios and mutation scenarios are added by extending the enums in stacked PRs.
 *
 * <p>Run: {@code build/sbt -batch 'sparkV2/testOnly io.delta.spark.internal.v2.V2StreamingMatrixTest'}
 */
public class V2StreamingMatrixTest extends V2StreamingMatrixTestBase {

  // ---------------------------------------------------------------------------
  // Table creation scenarios
  // ---------------------------------------------------------------------------

  /**
   * Each value creates a table with specific features and seed data. To add a new scenario, add an
   * enum value and implement the {@code create} method.
   */
  enum Setup {
    SIMPLE {
      @Override
      TableContext create(V2StreamingMatrixTestBase base, Path dir) {
        TableContext t = base.tableAt(dir, EnumSet.noneOf(TableFeature.class));
        base.spark.sql(str("CREATE TABLE %s (id INT, data STRING) USING delta", t.sqlIdent()));
        base.insertAppend(t, 0, 8);
        return t;
      }
    };

    abstract TableContext create(V2StreamingMatrixTestBase base, Path dir);
  }

  // ---------------------------------------------------------------------------
  // Mutation scenarios
  // ---------------------------------------------------------------------------

  /**
   * Each value applies a mutation to a table and declares its streaming behavior. To add a new
   * mutation, add an enum value and implement the three abstract methods.
   */
  enum Mutation {
    INSERT_APPEND {
      @Override
      boolean applicableTo(Setup s) {
        return true;
      }

      @Override
      boolean producesDataChangeRemoves() {
        return false;
      }

      @Override
      TableContext apply(V2StreamingMatrixTestBase base, TableContext t, Path workDir) {
        return base.insertAppend(t, 100, 5);
      }
    },

    OPTIMIZE {
      @Override
      boolean applicableTo(Setup s) {
        return true;
      }

      @Override
      boolean producesDataChangeRemoves() {
        // OPTIMIZE writes AddFile/RemoveFile with dataChange=false, so they are invisible to
        // streaming. This does NOT produce data-change removes.
        return false;
      }

      @Override
      TableContext apply(V2StreamingMatrixTestBase base, TableContext t, Path workDir) {
        base.writeSmallFileCommits(t, 4);
        base.spark.sql(str("OPTIMIZE %s", t.sqlIdent()));
        return t;
      }
    };

    /** Whether this mutation is valid for the given table setup. */
    abstract boolean applicableTo(Setup setup);

    /**
     * Whether this mutation produces {@code RemoveFile} with {@code dataChange=true}. If true, the
     * streaming source will error unless {@code skipChangeCommits} or {@code ignoreChanges} is set.
     */
    abstract boolean producesDataChangeRemoves();

    /** Apply the mutation to the table. Returns a (possibly updated) table context. */
    abstract TableContext apply(V2StreamingMatrixTestBase base, TableContext t, Path workDir);

    /**
     * The stream option required to survive this mutation during streaming. Returns {@code null} for
     * mutations that don't produce data-change removes.
     */
    String requiredStreamOption() {
      return producesDataChangeRemoves() ? "skipChangeCommits" : null;
    }
  }

  // ---------------------------------------------------------------------------
  // Matrix generation
  // ---------------------------------------------------------------------------

  static Stream<Arguments> postMutationCases() {
    return validCombinations();
  }

  static Stream<Arguments> streamThroughCases() {
    return validCombinations();
  }

  static Stream<Arguments> multiBatchCases() {
    return validCombinations();
  }

  /** Only mutations that produce data-change removes qualify for error testing. */
  static Stream<Arguments> errorCases() {
    return Arrays.stream(Setup.values())
        .flatMap(
            setup ->
                Arrays.stream(Mutation.values())
                    .filter(m -> m.applicableTo(setup))
                    .filter(Mutation::producesDataChangeRemoves)
                    .map(
                        m ->
                            Arguments.of(
                                str("%s/%s", setup.name(), m.name()), setup, m)));
  }

  private static Stream<Arguments> validCombinations() {
    return Arrays.stream(Setup.values())
        .flatMap(
            setup ->
                Arrays.stream(Mutation.values())
                    .filter(m -> m.applicableTo(setup))
                    .map(
                        m ->
                            Arguments.of(
                                str("%s/%s", setup.name(), m.name()), setup, m)));
  }

  // ---------------------------------------------------------------------------
  // Parameterized test methods
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "{0}")
  @MethodSource("postMutationCases")
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void testPostMutationRead(String name, Setup setup, Mutation mutation, @TempDir File tempDir)
      throws Exception {
    Path caseDir = tempDir.toPath().resolve(name.replace("/", "_"));
    java.nio.file.Files.createDirectories(caseDir);

    TableContext table = setup.create(this, caseDir);
    TableContext afterMutation = mutation.apply(this, table, caseDir);

    verifyPostMutationRead(afterMutation, caseDir, name);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("streamThroughCases")
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void testStreamThroughMutation(
      String name, Setup setup, Mutation mutation, @TempDir File tempDir) throws Exception {
    Path caseDir = tempDir.toPath().resolve(name.replace("/", "_"));
    java.nio.file.Files.createDirectories(caseDir);

    TableContext table = setup.create(this, caseDir);

    Map<String, String> options =
        mutation.requiredStreamOption() != null
            ? Collections.singletonMap(mutation.requiredStreamOption(), "true")
            : Collections.emptyMap();

    verifyStreamThroughMutation(
        table, () -> mutation.apply(this, table, caseDir), caseDir, name, options);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("multiBatchCases")
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void testMultiBatchRead(String name, Setup setup, Mutation mutation, @TempDir File tempDir)
      throws Exception {
    Path caseDir = tempDir.toPath().resolve(name.replace("/", "_"));
    java.nio.file.Files.createDirectories(caseDir);

    TableContext table = setup.create(this, caseDir);
    TableContext afterMutation = mutation.apply(this, table, caseDir);

    verifyMultiBatchRead(afterMutation, caseDir, name);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("errorCases")
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void testExpectedError(String name, Setup setup, Mutation mutation, @TempDir File tempDir)
      throws Exception {
    Path caseDir = tempDir.toPath().resolve(name.replace("/", "_"));
    java.nio.file.Files.createDirectories(caseDir);

    TableContext table = setup.create(this, caseDir);

    // Start stream with NO safety options, then apply the mutation — should error
    verifyStreamError(
        table,
        () -> mutation.apply(this, table, caseDir),
        caseDir,
        name,
        "Detected a data update");
  }
}
