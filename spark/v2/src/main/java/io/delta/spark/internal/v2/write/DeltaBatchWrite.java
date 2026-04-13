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
package io.delta.spark.internal.v2.write;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.*;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.internal.io.FileCommitProtocol;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.delta.files.DelayedCommitProtocol;
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker;
import org.apache.spark.sql.execution.datasources.WriteJobDescription;
import org.apache.spark.sql.execution.datasources.WriteTaskResult;
import org.apache.spark.sql.types.StructType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * DSv2 {@link BatchWrite} implementation for Delta tables.
 *
 * <p>Bridges Spark's standard file writing infrastructure with Kernel's transactional commit:
 *
 * <ul>
 *   <li>{@link #createBatchWriterFactory} returns {@link DeltaFileWriterFactory} which serializes
 *       the {@link WriteJobDescription} and {@link DelayedCommitProtocol} to executors. On
 *       executors, {@code SingleDirectoryDataWriter} or {@code DynamicPartitionDataSingleWriter}
 *       writes Parquet rows and {@code DelayedCommitProtocol} tracks files as V1 {@code AddFile}
 *       actions.
 *   <li>{@link #commit} collects V1 {@code AddFile} actions via {@code
 *       DelayedCommitProtocol.commitJob()}, converts them to Kernel {@link DataFileStatus}, groups
 *       by partition, and commits via {@link Transaction#commit}.
 * </ul>
 */
public class DeltaBatchWrite implements BatchWrite {

  private final Job job;
  private final WriteJobDescription description;
  private final DelayedCommitProtocol committer;
  private final Transaction txn;
  private final Engine engine;
  private final StructType partitionSchema;
  private final scala.Option<DeltaJobStatisticsTracker> deltaStatsTrackerOpt;
  private final io.delta.kernel.types.StructType dataFilePhysicalSchema;
  private final WriteMode writeMode;
  private final io.delta.kernel.expressions.Predicate replaceWherePredicate;
  private final Snapshot snapshot;

  DeltaBatchWrite(
      Job job,
      WriteJobDescription description,
      DelayedCommitProtocol committer,
      Transaction txn,
      Engine engine,
      StructType partitionSchema,
      scala.Option<DeltaJobStatisticsTracker> deltaStatsTrackerOpt,
      io.delta.kernel.types.StructType dataFilePhysicalSchema,
      WriteMode writeMode,
      io.delta.kernel.expressions.Predicate replaceWherePredicate,
      Snapshot snapshot) {
    this.job = job;
    this.description = description;
    this.committer = committer;
    this.txn = txn;
    this.engine = engine;
    this.partitionSchema = partitionSchema;
    this.deltaStatsTrackerOpt = deltaStatsTrackerOpt;
    this.dataFilePhysicalSchema = dataFilePhysicalSchema;
    this.writeMode = writeMode;
    this.replaceWherePredicate = replaceWherePredicate;
    this.snapshot = snapshot;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new DeltaFileWriterFactory(description, committer);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void commit(WriterCommitMessage[] messages) {
    // Phase 1: Standard V1 commit — extract WriteTaskResult and run commitJob.
    // DelayedCommitProtocol.commitJob() collects AddFile actions into addedStatuses.
    WriteTaskResult[] taskResults = new WriteTaskResult[messages.length];
    List<FileCommitProtocol.TaskCommitMessage> taskCommitMsgs = new ArrayList<>();
    for (int i = 0; i < messages.length; i++) {
      WriteTaskResult result = (WriteTaskResult) messages[i];
      taskResults[i] = result;
      taskCommitMsgs.add(result.commitMsg());
    }
    committer.commitJob(job, CollectionConverters.asScala(taskCommitMsgs).toSeq());

    // Phase 1.5: Process stats trackers (populates DeltaJobStatisticsTracker.recordedStats)
    DeltaStatsTrackerHelper.processStats(description.statsTrackers(), taskResults);

    // Build recorded stats lookup: file basename → JSON stats string
    Map<String, String> recordedStats =
        deltaStatsTrackerOpt.isDefined()
            ? DeltaStatsTrackerHelper.getRecordedStats(deltaStatsTrackerOpt.get())
            : Collections.emptyMap();

    // Phase 2: Convert V1 AddFile → Kernel DataFileStatus, grouped by partition.
    // Stats are matched by file basename. V1 alternatively injects stats into AddFile.copy()
    // via TransactionalWriteEdge, but since we convert to Kernel DataFileStatus anyway, it's
    // simpler to look up stats at conversion time rather than mutate the V1 actions.
    String tablePath = description.path();
    List<org.apache.spark.sql.delta.actions.AddFile> addedFiles =
        new ArrayList<>(CollectionConverters.asJava(committer.addedStatuses()));

    Row txnState = txn.getTransactionState(engine);

    // Group V1 AddFiles by their partition values
    Map<Map<String, String>, List<org.apache.spark.sql.delta.actions.AddFile>> grouped =
        new LinkedHashMap<>();
    for (org.apache.spark.sql.delta.actions.AddFile af : addedFiles) {
      Map<String, String> pv =
          Collections.unmodifiableMap(
              new HashMap<>(CollectionConverters.asJava(af.partitionValues())));
      grouped.computeIfAbsent(pv, k -> new ArrayList<>()).add(af);
    }

    // Phase 3: Generate Kernel append actions per partition group.
    List<Row> allActionRows = new ArrayList<>();
    for (Map.Entry<Map<String, String>, List<org.apache.spark.sql.delta.actions.AddFile>> entry :
        grouped.entrySet()) {
      Map<String, Literal> kernelPartValues = toKernelPartitionValues(entry.getKey());
      DataWriteContext writeContext =
          Transaction.getWriteContext(engine, txnState, kernelPartValues);

      List<DataFileStatus> dataFileStatuses = new ArrayList<>();
      for (org.apache.spark.sql.delta.actions.AddFile af : entry.getValue()) {
        String fileName = new Path(af.path()).getName();
        Optional<DataFileStatistics> fileStats = Optional.empty();
        if (recordedStats.containsKey(fileName)) {
          fileStats =
              DataFileStatistics.deserializeFromJson(
                  recordedStats.get(fileName), dataFilePhysicalSchema);
        }
        dataFileStatuses.add(
            new DataFileStatus(
                new Path(tablePath, af.path()).toString(),
                af.size(),
                af.modificationTime(),
                fileStats));
      }

      try (CloseableIterator<Row> actions =
          Transaction.generateAppendActions(
              engine, txnState, toCloseableIterator(dataFileStatuses.iterator()), writeContext)) {
        while (actions.hasNext()) {
          allActionRows.add(actions.next());
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to generate append actions", e);
      }
    }

    // Phase 4: Generate RemoveFile actions for overwrite modes.
    List<Row> removeActionRows = generateRemoveActions(addedFiles);

    // Phase 5: Kernel transactional commit — removes first, then adds.
    List<Row> allCommitActions = new ArrayList<>(removeActionRows.size() + allActionRows.size());
    allCommitActions.addAll(removeActionRows);
    allCommitActions.addAll(allActionRows);

    TransactionCommitResult result =
        txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(toCloseableIterator(allCommitActions.iterator())));

    // Phase 6: Post-commit hooks (e.g. checkpointing).
    for (PostCommitHook hook : result.getPostCommitHooks()) {
      try {
        hook.threadSafeInvoke(engine);
      } catch (java.io.IOException e) {
        throw new RuntimeException("Post-commit hook failed", e);
      }
    }
  }

  /**
   * Generates RemoveFile actions based on the write mode.
   *
   * <p>For APPEND, returns an empty list. For TRUNCATE, scans all existing files and generates
   * remove actions. For DYNAMIC_OVERWRITE, removes existing files only for partitions that appear
   * in the new data. For REPLACE_WHERE, removes files matching the replaceWhere predicate.
   */
  private List<Row> generateRemoveActions(
      List<org.apache.spark.sql.delta.actions.AddFile> newAddFiles) {
    if (writeMode == WriteMode.APPEND) {
      return Collections.emptyList();
    }

    List<Row> removeRows = new ArrayList<>();

    if (writeMode == WriteMode.TRUNCATE) {
      // Remove all existing files
      try (CloseableIterator<Row> scanFileIter =
          io.delta.kernel.internal.util.Utils.intoRows(
              snapshot.getScanBuilder().build().getScanFiles(engine))) {
        while (scanFileIter.hasNext()) {
          Row scanRow = scanFileIter.next();
          AddFile addFile = new AddFile(scanRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
          Row removeFileRow = addFile.toRemoveFileRow(true, Optional.empty());
          removeRows.add(SingleAction.createRemoveFileSingleAction(removeFileRow));
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan existing files for truncate", e);
      }
    } else if (writeMode == WriteMode.REPLACE_WHERE) {
      // Push the replaceWhere predicate into Kernel's ScanBuilder so only matching files
      // are returned — same pattern Iceberg uses (OverwriteFiles.overwriteByRowFilter).
      // Kernel evaluates the predicate against partition values and file-level stats.
      ScanBuilder scanBuilder = snapshot.getScanBuilder();
      if (replaceWherePredicate != null) {
        scanBuilder = scanBuilder.withFilter(replaceWherePredicate);
      }
      try (CloseableIterator<Row> scanFileIter =
          io.delta.kernel.internal.util.Utils.intoRows(scanBuilder.build().getScanFiles(engine))) {
        while (scanFileIter.hasNext()) {
          Row scanRow = scanFileIter.next();
          AddFile addFile = new AddFile(scanRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
          Row removeFileRow = addFile.toRemoveFileRow(true, Optional.empty());
          removeRows.add(SingleAction.createRemoveFileSingleAction(removeFileRow));
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan existing files for replaceWhere", e);
      }
    } else if (writeMode == WriteMode.DYNAMIC_OVERWRITE) {
      // Collect the set of partition values that appear in the new data
      Set<Map<String, String>> writtenPartitions = new HashSet<>();
      for (org.apache.spark.sql.delta.actions.AddFile af : newAddFiles) {
        Map<String, String> pv =
            Collections.unmodifiableMap(
                new HashMap<>(CollectionConverters.asJava(af.partitionValues())));
        writtenPartitions.add(pv);
      }

      // Scan all existing files, remove only those whose partition values match
      try (CloseableIterator<Row> scanFileIter =
          io.delta.kernel.internal.util.Utils.intoRows(
              snapshot.getScanBuilder().build().getScanFiles(engine))) {
        while (scanFileIter.hasNext()) {
          Row scanRow = scanFileIter.next();
          AddFile addFile = new AddFile(scanRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));
          Map<String, String> existingPartValues = mapValueToJavaMap(addFile.getPartitionValues());
          if (writtenPartitions.contains(existingPartValues)) {
            Row removeFileRow = addFile.toRemoveFileRow(true, Optional.empty());
            removeRows.add(SingleAction.createRemoveFileSingleAction(removeFileRow));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan existing files for dynamic overwrite", e);
      }
    }

    return removeRows;
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    committer.abortJob(job);
  }

  /**
   * Converts V1 string-encoded partition values to Kernel {@link Literal} values using the
   * partition schema's data types.
   */
  private Map<String, Literal> toKernelPartitionValues(Map<String, String> stringValues) {
    if (stringValues.isEmpty()) {
      return Collections.emptyMap();
    }
    io.delta.kernel.types.StructType kernelPartSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
            partitionSchema);
    Map<String, Literal> result = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : stringValues.entrySet()) {
      String colName = entry.getKey();
      String value = entry.getValue();
      StructField field = kernelPartSchema.get(colName);
      if (field == null) {
        throw new IllegalArgumentException("Unknown partition column: " + colName);
      }
      result.put(colName, stringToLiteral(value, field.getDataType()));
    }
    return result;
  }

  private static Literal stringToLiteral(String value, DataType type) {
    if (value == null) {
      return Literal.ofNull(type);
    }
    if (type instanceof IntegerType) {
      return Literal.ofInt(Integer.parseInt(value));
    } else if (type instanceof LongType) {
      return Literal.ofLong(Long.parseLong(value));
    } else if (type instanceof ShortType) {
      return Literal.ofShort(Short.parseShort(value));
    } else if (type instanceof ByteType) {
      return Literal.ofByte(Byte.parseByte(value));
    } else if (type instanceof FloatType) {
      return Literal.ofFloat(Float.parseFloat(value));
    } else if (type instanceof DoubleType) {
      return Literal.ofDouble(Double.parseDouble(value));
    } else if (type instanceof BooleanType) {
      return Literal.ofBoolean(Boolean.parseBoolean(value));
    } else if (type instanceof StringType) {
      return Literal.ofString(value);
    } else if (type instanceof DateType) {
      LocalDate date = LocalDate.parse(value);
      int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
      return Literal.ofDate(daysSinceEpoch);
    } else if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      return Literal.ofDecimal(new BigDecimal(value), dt.getPrecision(), dt.getScale());
    } else {
      // Fallback: treat as string
      return Literal.ofString(value);
    }
  }

  /** Converts a Kernel {@link MapValue} to a Java {@code Map<String, String>}. */
  private static Map<String, String> mapValueToJavaMap(MapValue mapValue) {
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < mapValue.getSize(); i++) {
      String key = mapValue.getKeys().getString(i);
      String value = mapValue.getValues().isNullAt(i) ? null : mapValue.getValues().getString(i);
      result.put(key, value);
    }
    return Collections.unmodifiableMap(result);
  }
}
