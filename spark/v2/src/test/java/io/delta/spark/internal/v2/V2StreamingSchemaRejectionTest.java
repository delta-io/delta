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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 mirrors of DSv1 streaming schema-rejection / CREATE-source DDL tests in {@code
 * DeltaSourceSuite.scala} (lines 96-220).
 *
 * <p>Each test corresponds 1:1 to a DSv1 case and asserts the *current* DSv2 behavior, with a
 * result classification embedded in the javadoc:
 *
 * <ul>
 *   <li>PASS — DSv2 behaves like DSv1 (the assertion captures the matching contract).
 *   <li>FAIL — DSv2 differs from DSv1 (the assertion captures the divergent DSv2 behavior; the
 *       comment names the parity gap and the DSv1 contract that's missing).
 *   <li>CANT-CONSTRUCT — the DSv1 input cannot be expressed via the DSv2 catalog API.
 * </ul>
 *
 * <p>The tests are written to pass against current DSv2 so they serve as a regression suite — if
 * DSv2 ever starts matching DSv1, the differing tests will fail and force re-classification.
 */
public class V2StreamingSchemaRejectionTest extends V2TestBase {

  /**
   * Mirrors DSv1 {@code "streaming delta source should not drop null columns"} (line 96).
   *
   * <p>DSv1 contract: with {@code DELTA_STREAMING_CREATE_DATAFRAME_DROP_NULL_COLUMNS = false}, the
   * source preserves a {@code VOID}/NullType column and the stream proceeds (the user's {@code
   * .drop("nullTypeCol")} succeeds before the writer sees the schema).
   *
   * <p><b>Classification: FAIL — parity gap.</b> DSv2 cannot even <i>load</i> a table that contains
   * a VOID column: {@code SparkTable} eagerly calls {@code snapshot.getSchema()} from Kernel, which
   * raises {@code KernelException("Failed to parse the schema. Encountered unsupported Delta data
   * type: VOID")}. So the test never reaches the streaming entrypoint, let alone exercises the
   * DROP_NULL_COLUMNS feature flag. The DSv1 "no drop" contract is unobservable on DSv2.
   *
   * <p><b>Bug shape:</b> Kernel-backed DSv2 catalog rejects any Delta table whose committed schema
   * mentions VOID/NullType, even though such tables are valid in DSv1 (Spark wrote them). This is
   * also a stronger blocker than DSv1: a user with a pre-existing table containing a VOID column
   * cannot read it via DSv2 at all.
   */
  @Test
  public void testCase1_streamingShouldNotDropNullColumns_v1Flag_false(
      @TempDir File sourceDir, @TempDir File sinkDir, @TempDir File checkpointDir) {
    String sourcePath = sourceDir.getAbsolutePath();

    spark
        .sql("select CAST(null as VOID) as nullTypeCol, id from range(10)")
        .write()
        .format("delta")
        .mode("append")
        .save(sourcePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", sourcePath);

    // ---- DSv2 leg: KernelException at loadTable. ----
    AtomicReference<Throwable> caught = new AtomicReference<>();
    withSQLConf(
        "spark.databricks.delta.streaming.unsafe.read.createDataFrame.dropNullColumns",
        "false",
        () -> {
          try {
            spark.readStream().table(dsv2TableRef);
          } catch (Throwable t) {
            caught.set(t);
          }
        });

    assertNotNull(caught.get(), "DSv2 should currently fail to even load a VOID-bearing table.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.contains("VOID") && msg.contains("unsupported Delta data type"),
        "Expected Kernel 'unsupported Delta data type: VOID' error, got: " + msg);

    // ---- DSv1 leg (documents the divergence): with the flag = false, DSv1 preserves the VOID
    // column and `.drop("nullTypeCol")` succeeds before the writer sees the schema; the stream
    // runs to completion.
    withSQLConf(
        "spark.databricks.delta.streaming.createDataFrame.dropNullColumns",
        "false",
        () ->
            assertDoesNotThrow(
                () -> {
                  Dataset<Row> v1Df =
                      spark.readStream().format("delta").load(sourcePath).drop("nullTypeCol");
                  StreamingQuery q =
                      v1Df.writeStream()
                          .option("checkpointLocation", checkpointDir.getAbsolutePath())
                          .format("delta")
                          .start(sinkDir.getAbsolutePath());
                  try {
                    q.processAllAvailable();
                  } finally {
                    q.stop();
                  }
                },
                "DSv1 with flag=false should run to completion (VOID column preserved, dropped by "
                    + "user)."));
  }

  /**
   * Mirrors DSv1 {@code "streaming delta source should drop null columns without feature flag"}
   * (line 100).
   *
   * <p>DSv1 contract: with the flag {@code true}, the source materializes the VOID column away; the
   * user's later {@code .drop("nullTypeCol")} fails because the column is already gone, surfacing
   * as {@code STREAM_FAILED} with {@code "assertion failed: Invalid batch: nullTypeCol"}.
   *
   * <p><b>Classification: FAIL — parity gap.</b> Same root cause as Case 1: DSv2 fails at {@code
   * loadTable} with {@code KernelException("unsupported Delta data type: VOID")} before reaching
   * the streaming source, so the DROP_NULL_COLUMNS flag (DSv1-only conf) cannot be observed on
   * DSv2.
   *
   * <p><b>Bug shape:</b> DSv2 misses the DROP_NULL_COLUMNS read-time materialization behavior; the
   * DSv1 conf {@code DELTA_STREAMING_CREATE_DATAFRAME_DROP_NULL_COLUMNS} is unwired in the DSv2
   * read path.
   */
  @Test
  public void testCase2_streamingShouldDropNullColumns_v1Flag_true(
      @TempDir File sourceDir, @TempDir File sinkDir, @TempDir File checkpointDir) {
    String sourcePath = sourceDir.getAbsolutePath();

    spark
        .sql("select CAST(null as VOID) as nullTypeCol, id from range(10)")
        .write()
        .format("delta")
        .mode("append")
        .save(sourcePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", sourcePath);

    // ---- DSv2 leg: KernelException at loadTable. ----
    AtomicReference<Throwable> caught = new AtomicReference<>();
    withSQLConf(
        "spark.databricks.delta.streaming.unsafe.read.createDataFrame.dropNullColumns",
        "true",
        () -> {
          try {
            spark.readStream().table(dsv2TableRef);
          } catch (Throwable t) {
            caught.set(t);
          }
        });

    assertNotNull(caught.get(), "DSv2 should currently fail to even load a VOID-bearing table.");
    String msg = unwrapMessages(caught.get());
    assertTrue(
        msg.contains("VOID") && msg.contains("unsupported Delta data type"),
        "Expected Kernel 'unsupported Delta data type: VOID' error, got: " + msg);

    // ---- DSv1 leg (documents the divergence): with the flag = true, DSv1 materializes the VOID
    // column away; the user's later `.drop("nullTypeCol")` fails because the column is already
    // gone, surfacing as "Invalid batch: nullTypeCol" via StreamingQueryException.
    Throwable v1Err =
        assertThrows(
            Throwable.class,
            () ->
                withSQLConf(
                    "spark.databricks.delta.streaming.createDataFrame.dropNullColumns",
                    "true",
                    () -> {
                      Dataset<Row> v1Df =
                          spark.readStream().format("delta").load(sourcePath).drop("nullTypeCol");
                      StreamingQuery q = null;
                      try {
                        q =
                            v1Df.writeStream()
                                .option("checkpointLocation", checkpointDir.getAbsolutePath())
                                .format("delta")
                                .start(sinkDir.getAbsolutePath());
                        q.processAllAvailable();
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      } finally {
                        if (q != null) {
                          try {
                            q.stop();
                          } catch (Exception ignored) {
                            // Already failed - best-effort cleanup.
                          }
                        }
                      }
                    }),
            "DSv1 with flag=true should fail because the dropped column is already gone.");
    String v1Msg = unwrapMessages(v1Err);
    assertTrue(
        v1Msg.contains("Invalid batch: nullTypeCol"),
        "Expected DSv1 'Invalid batch: nullTypeCol' error, got: " + v1Msg);
  }

  /**
   * Mirrors DSv1 {@code "no schema should throw an exception"} (line 104).
   *
   * <p>DSv1 contract: pointing the streaming reader at a directory with an empty {@code _delta_log}
   * raises {@code AnalysisException} with both {@code "Table schema is not set"} and {@code "CREATE
   * TABLE"} — a user-actionable error.
   *
   * <p><b>Classification: FAIL — parity gap on exception type and message.</b> DSv2 raises {@code
   * RuntimeException("Failed to load table: delta.`<path>`")} (wrapping a Kernel error) from {@code
   * TestCatalog.loadTable}. The exception type is a plain {@code RuntimeException} (not {@code
   * AnalysisException}) and the message contains neither {@code "Table schema is not set"} nor
   * {@code "CREATE TABLE"}.
   *
   * <p><b>Bug shape:</b> DSv2 throws a different exception type than DSv1 for the same input; the
   * message is also less actionable. Note: the {@code "Failed to load table"} prefix is specific to
   * {@code TestCatalog} (the test catalog used here) — production DSv2 catalogs may surface a
   * Kernel exception unwrapped, which is a separate, related parity gap.
   */
  @Test
  public void testCase3_noSchemaShouldThrow(@TempDir File inputDir) {
    new File(inputDir, "_delta_log").mkdir();
    String inputPath = inputDir.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", inputPath);

    // ---- DSv2 leg: throws, but with a non-actionable message. ----
    Throwable t =
        assertThrows(
            Throwable.class, () -> spark.readStream().table(dsv2TableRef).writeStream().toString());

    String msg = unwrapMessages(t);
    // DSv2 currently surfaces the test-catalog wrapping message, not the DSv1-style message.
    assertTrue(
        msg.contains("Failed to load table"),
        "Expected DSv2 'Failed to load table' message, got: " + msg);
    assertFalse(
        msg.contains("Table schema is not set") && msg.contains("CREATE TABLE"),
        "DSv2 currently does NOT match DSv1's actionable 'Table schema is not set / CREATE TABLE' "
            + "message. If this assertion fails, parity has been restored - re-classify Case 3 as "
            + "PASS.");

    // ---- DSv1 leg: also throws, with the actionable message DSv2 lacks. ----
    Throwable v1Err =
        assertThrows(Throwable.class, () -> spark.readStream().format("delta").load(inputPath));
    String v1Msg = unwrapMessages(v1Err);
    assertTrue(
        v1Msg.contains("Table schema is not set") && v1Msg.contains("CREATE TABLE"),
        "Expected DSv1 'Table schema is not set' + 'CREATE TABLE' error, got: " + v1Msg);
  }

  /**
   * Mirrors DSv1 {@code "disallow user specified schema"} (line 116) — the *mismatched* schema
   * variant.
   *
   * <p>DSv1 contract: providing a user schema that differs from the Delta table schema raises
   * {@code AnalysisException} with {@code "The schema provided for the source read doesn't match
   * the schema of the Delta table"} (error class {@code DELTA_READ_SOURCE_SCHEMA_CONFLICT}).
   *
   * <p><b>Classification: FAIL — bug found (silent acceptance).</b> DSv2 silently accepts a
   * mismatched user schema via {@code spark.readStream().schema(userSchema).table(dsv2TableRef)};
   * no exception is thrown at planning time. This is exactly the bug shape called out in the task
   * spec: "DSv2 silently accepts a user-specified schema that doesn't match (no error)".
   *
   * <p>Whether the divergent schema then causes a runtime read error is a separate question; the
   * parity gap with DSv1 is at the planning/check stage.
   */
  @Test
  public void testCase4a_disallowUserSchema_mismatched(@TempDir File inputDir) {
    String tablePath = inputDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (value STRING) USING delta", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    StructType userSchema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("a", DataTypes.IntegerType, true),
                DataTypes.createStructField("b", DataTypes.StringType, true)));

    // ---- DSv2 leg: silently accepts the mismatched user schema (BUG). ----
    Dataset<Row> df =
        assertDoesNotThrow(
            () -> spark.readStream().schema(userSchema).table(dsv2TableRef),
            "DSv2 currently silently accepts a mismatched user-specified schema. If this "
                + "assertion fails (DSv2 now throws), the parity gap with DSv1's "
                + "DELTA_READ_SOURCE_SCHEMA_CONFLICT has been closed - re-classify Case 4a as "
                + "PASS.");
    assertNotNull(df);
    assertTrue(df.isStreaming());

    // ---- DSv1 leg: rejects with DELTA_READ_SOURCE_SCHEMA_CONFLICT. ----
    Throwable v1Err =
        assertThrows(
            Throwable.class,
            () -> spark.readStream().schema(userSchema).format("delta").load(tablePath),
            "DSv1 should reject a mismatched user-specified schema.");
    String v1Msg = unwrapMessages(v1Err);
    assertTrue(
        v1Msg.contains(
            "The schema provided for the source read doesn't match the schema of the Delta "
                + "table"),
        "Expected DSv1 DELTA_READ_SOURCE_SCHEMA_CONFLICT error, got: " + v1Msg);
  }

  /**
   * Mirrors DSv1 {@code "disallow user specified schema"} (line 116) — the *matching* schema
   * variant.
   *
   * <p>DSv1 contract: even when the user-supplied schema *matches* the table schema, the public
   * {@code spark.readStream.schema(...).format("delta").load(...)} entry point still rejects with
   * {@code "does not support user-specified schema"}.
   *
   * <p><b>Classification: FAIL — bug found (silent acceptance).</b> DSv2's {@code .table(...)}
   * entry point silently accepts {@code .schema(...)}; no exception is thrown.
   *
   * <p>Note: DSv1 and DSv2 use different DataStreamReader entry points ({@code .load(path)} vs
   * {@code .table(name)}), so Spark itself routes them differently. The parity question is whether
   * the *Delta* connector enforces "no user schema" on its DSv2 path; currently it does not.
   */
  @Test
  public void testCase4b_disallowUserSchema_matching(@TempDir File inputDir) {
    String tablePath = inputDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (value STRING) USING delta", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    StructType matchingSchema =
        DataTypes.createStructType(
            Arrays.asList(DataTypes.createStructField("value", DataTypes.StringType, true)));

    // ---- DSv2 leg: silently accepts the user-specified schema (BUG). ----
    Dataset<Row> df =
        assertDoesNotThrow(
            () -> spark.readStream().schema(matchingSchema).table(dsv2TableRef),
            "DSv2 currently silently accepts a (matching) user-specified schema. If this "
                + "assertion fails (DSv2 now throws), the parity gap with DSv1's 'does not "
                + "support user-specified schema' contract has been closed - re-classify Case "
                + "4b as PASS.");
    assertNotNull(df);
    assertTrue(df.isStreaming());

    // ---- DSv1 leg: rejects even a matching user schema via .load(path). ----
    Throwable v1Err =
        assertThrows(
            Throwable.class,
            () -> spark.readStream().schema(matchingSchema).format("delta").load(tablePath),
            "DSv1 should reject any user-specified schema on the .load(path) entry point.");
    String v1Msg = unwrapMessages(v1Err);
    assertTrue(
        v1Msg.contains("does not support user-specified schema"),
        "Expected DSv1 'does not support user-specified schema' error, got: " + v1Msg);
  }

  /**
   * Mirrors DSv1 {@code "allow user specified schema if consistent: v1 source"} (line 144).
   *
   * <p>DSv1 contract: an internal {@code DataSource(spark, userSpecifiedSchema=Some(...),
   * className="delta", ...)} succeeds when the user schema matches. This is a Spark-internal
   * advanced-plugin API.
   *
   * <p><b>Classification: CANT-CONSTRUCT.</b> DSv2 has no equivalent {@code DataSource(...,
   * userSpecifiedSchema=Some(...))} entry point. The advanced-plugin surface is gone in DSv2; the
   * only public entry point is {@code .readStream().table(...)}, which is exercised by Cases 4a/4b.
   * There is no DSv2 internal API that takes a userSpecifiedSchema and bypasses the catalog.
   */
  @Test
  public void testCase5_allowUserSchemaIfConsistent_v1Source_cantConstruct() {
    // CANT-CONSTRUCT — documented above. Test passes trivially.
  }

  /**
   * Mirrors DSv1 {@code "createSource should create source with empty or matching table schema
   * provided"} (line 162).
   *
   * <p>DSv1 contract: {@code DeltaDataSource.createSource(...)} (a {@code StreamSourceProvider}
   * implementation) accepts {@code Some(emptySchema)} or {@code Some(matchingSchema)} but rejects
   * mismatched / extra-field schemas with {@code DELTA_READ_SOURCE_SCHEMA_CONFLICT}.
   *
   * <p><b>Classification: CANT-CONSTRUCT.</b> {@code DeltaDataSource.createSource} is a DSv1-only
   * API on Spark's {@code StreamSourceProvider}. DSv2 streaming sources are constructed via {@code
   * SparkTable.newScanBuilder} -> {@code SparkScanBuilder.build} -> {@code
   * SparkScan.toMicroBatchStream} and never accept a user-supplied schema. The
   * "schema=Some(emptySchema) is allowed" semantics also have no public DSv2 analogue: {@code
   * DataStreamReader.schema(...)} requires a non-empty {@link StructType}.
   *
   * <p>The end-user observable mismatched-schema behavior <i>is</i> covered by Case 4a above. This
   * test only documents that the lower-level API no longer exists.
   */
  @Test
  public void testCase6_createSourceMatchingOrEmpty_cantConstruct(@TempDir File inputDir) {
    String tablePath = inputDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT NOT NULL, name STRING) USING delta", tablePath));
    // Sanity: table loads via DSv2 with no user-supplied schema.
    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    assertTrue(df.isStreaming(), "DSv2 streaming DF should construct without user schema.");
    // CANT-CONSTRUCT — DSv2 has no StreamSourceProvider.createSource analogue.
  }

  /**
   * Walks the cause chain and concatenates messages so .contains() checks can match across wrapped
   * exceptions.
   */
  private static String unwrapMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    while (cur != null) {
      sb.append(cur.getClass().getName())
          .append(": ")
          .append(cur.getMessage() == null ? "" : cur.getMessage())
          .append("\n");
      cur = cur.getCause();
    }
    return sb.toString();
  }
}
