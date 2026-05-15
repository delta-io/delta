package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
      Set.of(DeltaLogActionUtils.DeltaAction.ADD, DeltaLogActionUtils.DeltaAction.REMOVE);
  private static final String INSERT_CHANGE_TYPE = "insert";
  private static final String DELETE_CHANGE_TYPE = "delete";

  private final CommitRange commitRange;
  private final Engine engine;
  private final StructType dataSchema;
  private final Snapshot snapshot;
  private final boolean rowTrackingEnabled;
  private final Configuration hadoopConf;

  public DeltaChangelogBatch(
      CommitRange commitRange,
      Engine engine,
      StructType dataSchema,
      Snapshot snapshot,
      boolean rowTrackingEnabled,
      Configuration hadoopConf) {
    this.commitRange = commitRange;
    this.engine = engine;
    this.dataSchema = dataSchema;
    this.snapshot = snapshot;
    this.rowTrackingEnabled = rowTrackingEnabled;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<InputPartition> partitions = new ArrayList<>();

    // TODO Remove streaminghelper usage
    try (CloseableIterator<CommitActions> commitsIter =
        StreamingHelper.getCommitActionsFromRangeUnsafe(
            engine, (CommitRangeImpl) commitRange, snapshot.getPath(), CHANGELOG_ACTION_SET)) {
      while (commitsIter.hasNext()) {
        // For a single commit, emit DELETE partitions (RemoveFiles) before INSERT partitions
        // (AddFiles). The Delta commit-log action order is not contract for downstream readers
        // (e.g. Spark's batch CDC post-processor) — it expects the preimage-then-postimage
        // ordering when forming update pairs. Buffering per-commit lets us stabilize this
        // regardless of how AddFile/RemoveFile are interleaved in the on-disk commit log.
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
                long baseRowId =
                    rowTrackingEnabled
                        ? add.getBaseRowId()
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        "AddFile "
                                            + add.getPath()
                                            + " missing baseRowId on row-tracking-enabled table"))
                        : 0L;
                long defaultRcv =
                    rowTrackingEnabled
                        ? add.getDefaultRowCommitVersion()
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        "AddFile "
                                            + add.getPath()
                                            + " missing defaultRowCommitVersion on row-tracking-enabled table"))
                        : 0L;
                CDCInputPartition cdcPartition =
                    new CDCInputPartition(
                        add.getPath(),
                        add.getSize(),
                        commit.getVersion(),
                        commit.getTimestamp(),
                        INSERT_CHANGE_TYPE,
                        baseRowId,
                        defaultRcv);
                commitAdds.add(cdcPartition);
              }
              Optional<RemoveFile> removeOpt = StreamingHelper.getDataChangeRemove(batch, rowId);
              if (removeOpt.isPresent()) {
                RemoveFile remove = removeOpt.get();
                long baseRowId =
                    rowTrackingEnabled
                        ? remove
                            .getBaseRowId()
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        "RemoveFile "
                                            + remove.getPath()
                                            + " missing baseRowId on row-tracking-enabled table"))
                        : 0L;
                long defaultRcv =
                    rowTrackingEnabled
                        ? remove
                            .getDefaultRowCommitVersion()
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        "RemoveFile "
                                            + remove.getPath()
                                            + " missing defaultRowCommitVersion on row-tracking-enabled table"))
                        : 0L;
                CDCInputPartition cdcPartition =
                    new CDCInputPartition(
                        remove.getPath(),
                        remove.getSize().orElse(0L),
                        commit.getVersion(),
                        commit.getTimestamp(),
                        DELETE_CHANGE_TYPE,
                        baseRowId,
                        defaultRcv);
                commitRemoves.add(cdcPartition);
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to process CDC commit actions", e);
        }
        partitions.addAll(commitRemoves);
        partitions.addAll(commitAdds);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan CDC input partitions", e);
    }
    return partitions.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    StructType partitionSchema = new StructType();
    StructType readDataSchema = dataSchema;
    if (rowTrackingEnabled) {
      readDataSchema =
          readDataSchema.add(DeltaChangelog.METADATA_COLUMN, DeltaChangelog.METADATA_STRUCT, false);
    }
    Filter[] dataFilters = new Filter[0];
    scala.collection.immutable.Map<String, String> scalaOptions =
        scala.collection.immutable.Map$.MODULE$.empty();
    SQLConf sqlConf = SQLConf.get();

    // BATCH_CHANGELOG: PartitionUtils does NOT inject CDC tail columns or wrap the reader with
    // CDCReadFunction here. Auto-CDF's outer CDCPartitionReaderFactory below appends
    // _change_type / _commit_version / _commit_timestamp as per-partition constants instead.
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
            io.delta.spark.internal.v2.read.cdc.CdcReadMode.BATCH_CHANGELOG);

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
      // Tail values are partition-constants — set them once at construction so
      // get() doesn't redo the same writes on every row.
      this.cdcTail.update(0, UTF8String.fromString(cdcPartition.getChangeType()));
      this.cdcTail.setLong(1, cdcPartition.getCommitVersion());
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
