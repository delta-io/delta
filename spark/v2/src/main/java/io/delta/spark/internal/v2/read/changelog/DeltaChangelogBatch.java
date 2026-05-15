package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;

public class DeltaChangelogBatch implements Batch {
  private static final Set<DeltaLogActionUtils.DeltaAction> CHANGELOG_ACTION_SET =
      Set.of(
          DeltaLogActionUtils.DeltaAction.ADD,
          DeltaLogActionUtils.DeltaAction.REMOVE,
          DeltaLogActionUtils.DeltaAction.METADATA);
  private static final String INSERT_CHANGE_TYPE = "insert";
  private static final String DELETE_CHANGE_TYPE = "delete";

  private final CommitRange commitRange;
  private final Engine engine;
  private final StructType dataSchema;
  private final Snapshot snapshot;
  private final Configuration hadoopConf;

  public DeltaChangelogBatch(
      CommitRange commitRange,
      Engine engine,
      StructType dataSchema,
      Snapshot snapshot,
      Configuration hadoopConf) {
    this.commitRange = commitRange;
    this.engine = engine;
    this.dataSchema = dataSchema;
    this.snapshot = snapshot;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<InputPartition> partitions = new ArrayList<>();

    // Eager schema-drift check: if the start-version snapshot's schema differs from the
    // end-version reference (passed in as `dataSchema`), a Metadata-changing commit lies in
    // the range. The per-commit Metadata loop below catches the case where Metadata also
    // appears inside the range; this start-vs-end pre-check catches the case where the
    // schema before our first iterated commit is already out of sync.
    StructType startSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    if (!startSchema.equals(dataSchema)) {
      DeltaErrors.throwChangelogSchemaChangeInRange(
          ((CommitRangeImpl) commitRange).getStartVersion());
    }

    // TODO: Remove StreamingHelper usage; the helper is generic, only the class name is
    // streaming-flavored.
    try (CloseableIterator<CommitActions> commitsIter =
        StreamingHelper.getCommitActionsFromRangeUnsafe(
            engine, (CommitRangeImpl) commitRange, snapshot.getPath(), CHANGELOG_ACTION_SET)) {
      while (commitsIter.hasNext()) {
        // The SPIP analyzer re-partitions and re-sorts by (rowId, rowVersion) before the CDC
        // post-processor inspects pairs, so it does not require any particular partition order
        // from the connector. Direct-batch tests that bypass the analyzer do iterate partitions
        // in emission order, though; emitting RemoveFiles before AddFiles per commit gives
        // those tests a deterministic preimage-then-postimage shape.
        List<InputPartition> commitRemoves = new ArrayList<>();
        List<InputPartition> commitAdds = new ArrayList<>();
        try (CommitActions commit = commitsIter.next();
            CloseableIterator<ColumnarBatch> actionsIter = commit.getActions()) {
          while (actionsIter.hasNext()) {
            ColumnarBatch batch = actionsIter.next();
            for (int rowId = 0; rowId < batch.getSize(); rowId++) {
              Optional<AddFile> addOpt = StreamingHelper.getAddFileWithDataChange(batch, rowId);
              if (addOpt.isPresent()) {
                AddFile add = addOpt.get();
                commitAdds.add(
                    buildPartition(
                        add.getPath(),
                        add.getSize(),
                        INSERT_CHANGE_TYPE,
                        commit.getVersion(),
                        commit.getTimestamp(),
                        add::getBaseRowId,
                        add::getDefaultRowCommitVersion,
                        "AddFile"));
              }
              Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
              if (removeOpt.isPresent()) {
                RemoveFile remove = removeOpt.get();
                commitRemoves.add(
                    buildPartition(
                        remove.getPath(),
                        remove.getSize().orElse(0L),
                        DELETE_CHANGE_TYPE,
                        commit.getVersion(),
                        commit.getTimestamp(),
                        remove::getBaseRowId,
                        remove::getDefaultRowCommitVersion,
                        "RemoveFile"));
              }
              // Validate Metadata actions: schema and row-tracking config must match the
              // end-version baseline established by DeltaChangelogScanBuilder. Mid-range
              // schema evolution or row-tracking-toggle would silently corrupt downstream
              // CDC post-processing (row identity / column mapping drift).
              Optional<Metadata> metadataOpt = StreamingHelper.getMetadata(batch, rowId);
              if (metadataOpt.isPresent()) {
                Metadata md = metadataOpt.get();
                StructType commitSchema =
                    SchemaUtils.convertKernelSchemaToSparkSchema(md.getSchema());
                if (!commitSchema.equals(dataSchema)) {
                  DeltaErrors.throwChangelogSchemaChangeInRange(commit.getVersion());
                }
                String rtValue = md.getConfiguration().get("delta.enableRowTracking");
                // Absent key means the prior value persists (no change at this commit).
                boolean rowTrackingEnabled = rtValue == null || "true".equalsIgnoreCase(rtValue);
                if (!rowTrackingEnabled) {
                  DeltaErrors.throwChangelogRowTrackingDisabledInRange(commit.getVersion());
                }
              }
            }
          }
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          // try-with-resources requires catching Exception because CommitActions.close()
          // declares it. Unchecked exceptions (e.g. the IllegalStateExceptions thrown above
          // by buildPartition's orElseThrow) are re-thrown unchanged.
          throw new RuntimeException("Failed to process CDC commit actions", e);
        }
        partitions.addAll(commitRemoves);
        partitions.addAll(commitAdds);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan CDC input partitions", e);
    }
    return partitions.toArray(new InputPartition[0]);
  }

  /**
   * Build a {@link CDCInputPartition} for a single AddFile or RemoveFile action. Both action types
   * require non-empty {@code baseRowId} and {@code defaultRowCommitVersion}; the caller (catalog)
   * has already validated that row tracking is enabled at the start version of the read, so any
   * missing value here is an invariant violation rather than a user-facing error.
   */
  private static CDCInputPartition buildPartition(
      String path,
      long size,
      String changeType,
      long commitVersion,
      long commitTimestampMillis,
      Supplier<Optional<Long>> baseRowIdAccessor,
      Supplier<Optional<Long>> defaultRowCommitVersionAccessor,
      String actionDescription) {
    long baseRowId =
        baseRowIdAccessor
            .get()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        actionDescription + " " + path + " missing baseRowId"));
    long defaultRcv =
        defaultRowCommitVersionAccessor
            .get()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        actionDescription + " " + path + " missing defaultRowCommitVersion"));
    return new CDCInputPartition(
        path, size, commitVersion, commitTimestampMillis, changeType, baseRowId, defaultRcv);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    StructType partitionSchema = new StructType();
    StructType readDataSchema =
        dataSchema.add(DeltaChangelog.METADATA_COLUMN, DeltaChangelog.METADATA_STRUCT, false);
    Filter[] dataFilters = new Filter[0];
    scala.collection.immutable.Map<String, String> scalaOptions =
        scala.collection.immutable.Map$.MODULE$.empty();
    SQLConf sqlConf = SQLConf.get();

    // Read-time Auto-CDF reads raw parquet here. The CDC tail columns (_change_type,
    // _commit_version, _commit_timestamp) are added below by CDCPartitionReaderFactory as
    // per-partition constants, not by PartitionUtils. This is unrelated to write-time CDF
    // (streaming with readChangeFeed=true), which is the only consumer of the
    // isWriteTimeCDCRead branch in PartitionUtils.
    PartitionReaderFactory delegate =
        PartitionUtils.createDeltaParquetReaderFactory(
            snapshot,
            dataSchema,
            partitionSchema,
            readDataSchema,
            /* ddlOrderedReadOutputSchema */ readDataSchema,
            dataFilters,
            scalaOptions,
            hadoopConf,
            sqlConf,
            /* isWriteTimeCDCRead */ false);

    StructType outputSchema =
        readDataSchema
            .add("_change_type", DataTypes.StringType, false)
            .add("_commit_version", DataTypes.LongType, false)
            .add("_commit_timestamp", DataTypes.TimestampType, false);
    return new CDCPartitionReaderFactory(delegate, snapshot.getPath(), outputSchema);
  }

  /** Serialized to executors; represents one CDC file change unit. */
  static class CDCInputPartition implements InputPartition, Serializable {
    private final String filePath;
    private final long fileSize;
    private final long commitVersion;
    private final long commitTimestampMillis;
    private final String changeType;
    private final long baseRowId;
    private final long defaultRowCommitVersion;

    CDCInputPartition(
        String filePath,
        long fileSize,
        long commitVersion,
        long commitTimestampMillis,
        String changeType,
        long baseRowId,
        long defaultRowCommitVersion) {
      this.filePath = filePath;
      this.fileSize = fileSize;
      this.commitVersion = commitVersion;
      this.commitTimestampMillis = commitTimestampMillis;
      this.changeType = changeType;
      this.baseRowId = baseRowId;
      this.defaultRowCommitVersion = defaultRowCommitVersion;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getCommitVersion() {
      return commitVersion;
    }

    public long getCommitTimestampMillis() {
      return commitTimestampMillis;
    }

    public String getChangeType() {
      return changeType;
    }

    public long getBaseRowId() {
      return baseRowId;
    }

    public long getDefaultRowCommitVersion() {
      return defaultRowCommitVersion;
    }
  }

  /** Executor-side factory for CDC partition readers. */
  static class CDCPartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private final PartitionReaderFactory delegate;
    private final String tablePath;
    private final StructType outputSchema;

    CDCPartitionReaderFactory(
        PartitionReaderFactory delegate, String tablePath, StructType outputSchema) {
      this.delegate = delegate;
      this.tablePath = tablePath;
      this.outputSchema = outputSchema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      CDCInputPartition cdcPartition = (CDCInputPartition) partition;
      InternalRow partitionValues = new GenericInternalRow(0);
      SparkPath sparkPath =
          SparkPath.fromUrlString(new Path(tablePath, cdcPartition.getFilePath()).toString());
      scala.collection.immutable.Map<String, Object> constantMetadata =
          (scala.collection.immutable.Map<String, Object>)
              (scala.collection.immutable.Map<?, ?>)
                  scala.collection.immutable.Map$.MODULE$.empty();
      constantMetadata =
          constantMetadata.$plus(
              new Tuple2<>(RowId$.MODULE$.BASE_ROW_ID(), cdcPartition.getBaseRowId()));
      constantMetadata =
          constantMetadata.$plus(
              new Tuple2<>(
                  DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(),
                  cdcPartition.getDefaultRowCommitVersion()));

      PartitionedFile file =
          new PartitionedFile(
              partitionValues,
              sparkPath,
              /* start */ 0L,
              /* length */ cdcPartition.getFileSize(),
              /* locations */ new String[0],
              /* modificationTime */ cdcPartition.getCommitTimestampMillis(),
              /* fileSize */ cdcPartition.getFileSize(),
              constantMetadata);
      FilePartition filePartition = new FilePartition(0, new PartitionedFile[] {file});
      PartitionReader<InternalRow> baseReader = delegate.createReader(filePartition);
      return new CDCPartitionReader(baseReader, cdcPartition, outputSchema);
    }
  }

  /** Executor-side reader stub for per-file CDC row materialization. */
  static class CDCPartitionReader implements PartitionReader<InternalRow> {
    private final PartitionReader<InternalRow> baseReader;
    private final UnsafeProjection projection;
    private final GenericInternalRow cdcTail = new GenericInternalRow(3);
    private final JoinedRow joined = new JoinedRow();

    CDCPartitionReader(
        PartitionReader<InternalRow> baseReader,
        CDCInputPartition cdcPartition,
        StructType outputSchema) {
      this.baseReader = baseReader;
      this.projection = UnsafeProjection.create(outputSchema);
      // Tail values are partition-constants. Set them once at construction so get() does not
      // redo the same writes on every row.
      this.cdcTail.update(0, UTF8String.fromString(cdcPartition.getChangeType()));
      this.cdcTail.setLong(1, cdcPartition.getCommitVersion());
      // millis to micros: Catalyst stores TimestampType as microseconds since epoch.
      this.cdcTail.setLong(2, cdcPartition.getCommitTimestampMillis() * 1000L);
    }

    @Override
    public boolean next() throws IOException {
      return baseReader.next();
    }

    @Override
    public InternalRow get() {
      return projection.apply(joined.apply(baseReader.get(), cdcTail));
    }

    @Override
    public void close() throws IOException {
      baseReader.close();
    }
  }
}
