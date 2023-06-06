package io.delta.flink.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.Metadata.Builder;

public class DeltaTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaTestUtils.class);

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    /**
     * Set up a simple hdfs mock as default filesystem. This FS should not be used by reference
     * of DeltaLog. If used, and Delta log will use default filesystem (mockfs:///) path,
     * it would return a null. This allows to verify that full paths, including schema are used
     * and passed around.
     */
    public static org.apache.hadoop.conf.Configuration getConfigurationWithMockFs() {
        org.apache.hadoop.conf.Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        hadoopConf.set("fs.defaultFS", "mockfs:///");
        hadoopConf.setClass("fs.mockfs.impl",
            FileSystemTestHelper.MockFileSystem.class, FileSystem.class);

        return hadoopConf;
    }

    ///////////////////////////////////////////////////////////////////////////
    // test data utils
    ///////////////////////////////////////////////////////////////////////////

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR =
        "/test-data/test-non-partitioned-delta-table-initial-state";

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_DIR =
        "/test-data/test-partitioned-delta-table-initial-state";

    public static final String TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR =
        "/test-data/test-non-partitioned-delta-table_1100_records";

    public static final String TEST_DELTA_TABLE_ALL_DATA_TYPES =
        "/test-data/test-non-partitioned-delta-table-alltypes";

    public static final String TEST_VERSIONED_DELTA_TABLE =
        "/test-data/test-non-partitioned-delta-table-4-versions";

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_TABLE_API_DIR =
        "/test-data/test-table-api";

    public static void initTestForAllDataTypes(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_ALL_DATA_TYPES, targetTablePath);
    }

    public static void initTestForNonPartitionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR, targetTablePath);
    }

    public static void initTestForPartitionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_INITIAL_STATE_P_DIR, targetTablePath);
    }

    public static void initTestForNonPartitionedLargeTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR, targetTablePath);
    }

    public static void initTestForVersionedTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_VERSIONED_DELTA_TABLE, targetTablePath);
    }

    public static void initTestForTableApiTable(String targetTablePath)
        throws IOException {
        initTestFor(TEST_DELTA_TABLE_INITIAL_STATE_TABLE_API_DIR, targetTablePath);
    }

    public static void initTestFor(String testDeltaTableInitialStateNpDir, String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + testDeltaTableInitialStateNpDir;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    /**
     * In this method we check in short time intervals for the total time of 10 seconds whether
     * the DeltaLog for the table has been already created by the Flink job running in the deamon
     * thread.
     *
     * @param deltaLog {@link DeltaLog} instance for test table
     * @throws InterruptedException when the thread is interrupted when waiting for the log to be
     *                              created
     */
    public static void waitUntilDeltaLogExists(DeltaLog deltaLog) throws InterruptedException {
        waitUntilDeltaLogExists(deltaLog, 0L);
    }

    /**
     * In this method we check in short time intervals for the total time of 20 seconds whether
     * the DeltaLog for the table has been already created by the Flink job running in the deamon
     * thread and whether the table version is equal or higher than specified.
     *
     * @param deltaLog {@link DeltaLog} instance for test table
     * @param minVersion minimum version of the table
     * @throws InterruptedException when the thread is interrupted when waiting for the log to be
     *                              created
     */
    public static void waitUntilDeltaLogExists(DeltaLog deltaLog, Long minVersion)
        throws InterruptedException {
        int i = 0;
        while (deltaLog.snapshot().getVersion() < minVersion) {
            if (i > 20) throw new RuntimeException(
                "Timeout. DeltaLog for table has not been initialized");
            i++;
            Thread.sleep(1000);
            deltaLog.update();
        }
    }

    public static void triggerFailover(FailoverType type, JobID jobId, Runnable afterFailAction,
        MiniCluster miniCluster) throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TASK_MANAGER:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JOB_MANAGER:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    public static void triggerJobManagerFailover(
        JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        LOG.info("Triggering Job Manager failover.");
        HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
        throws Exception {
        LOG.info("Triggering Task Manager failover.");
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    public static MiniClusterWithClientResource buildCluster(int slotPerTaskManager) {
        Configuration configuration = new Configuration();

        // By default, let's check for leaked classes in tests.
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);

        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(slotPerTaskManager)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .setConfiguration(configuration)
                .build());
    }

    public static <T> List<T> testBoundedStream(
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource)
        throws Exception {

        return testBoundedStream(
            FailoverType.NONE,
            (FailCheck) integer -> true,
            stream,
            miniClusterResource
        );
    }

    /**
     * A utility method to test bounded {@link DataStream} with failover scenarios.
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param stream       The {@link DataStream} under test.
     * @param miniClusterResource          The {@link MiniClusterWithClientResource} where given
     *                                     stream under test is executed.
     * @return A {@link List} of produced records.
     * <p>
     * @implNote The {@code RecordCounterToFail::wrapWithFailureAfter} for every row checks the
     * "fail check" and if true and if this is a first fail check it completes the FAIL {@code
     * CompletableFuture} and waits on continueProcessing {@code CompletableFuture} next.
     * <p>
     * The flow is:
     * <ul>
     *      <li>
     *          The main test thread creates Flink's Streaming Environment.
     *      </li>
     *      <li>
     *          The main test thread creates Delta source.
     *      </li>
     *      <li>
     *          The main test thread wraps created source with {@code wrapWithFailureAfter} which
     *          has the {@code FailCheck} condition.
     *      </li>
     *      <li>
     *          The main test thread starts the "test Flink cluster" to produce records from
     *          Source via {@code DataStreamUtils.collectWithClient(...)}. As a result there is a
     *          Flink mini cluster created and data is consumed by source on a new thread.
     *      </li>
     *      <li>
     *          The main thread waits for "fail signal" that is issued by calling fail
     *          .complete. This is done on that new thread from point above. After calling {@code
     *          fail.complete} the source thread waits on {@code continueProcessing.get()};
     *       </li>
     *       <li>
     *           When the main thread sees that fail.complete was executed by the Source
     *          thread, it triggers the "generic" failover based on failoverType by calling
     *          {@code triggerFailover(
     *          ...)}.
     *      </li>
     *      <li>
     *          After failover is complied, the main thread calls
     *          {@code RecordCounterToFail::continueProcessing},
     *          which releases the Source thread and resumes record consumption.
     *       </li>
     * </ul>
     * For test where FailoverType == NONE, we trigger fail signal on a first record, Main thread
     * executes triggerFailover method which only sends a continueProcessing signal that resumes
     * the Source thread.
     */
    public static <T> List<T> testBoundedStream(
            FailoverType failoverType,
            FailCheck failCheck,
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource)
        throws Exception {

        DataStream<T> failingStreamDecorator =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(
                failingStreamDecorator, "Bounded Delta Source Test");
        JobID jobId = client.client.getJobID();

        // Wait with main thread until FailCheck from RecordCounterToFail.wrapWithFailureAfter
        // triggers.
        RecordCounterToFail.waitToFail();

        // Trigger The Failover with desired failover failoverType and continue processing after
        // recovery.
        DeltaTestUtils.triggerFailover(
            failoverType,
            jobId,
            RecordCounterToFail::continueProcessing,
            miniClusterResource.getMiniCluster());

        final List<T> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }

        return result;
    }

    /**
     * A utility method to test unbounded {@link DataStream} with failover scenarios.
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType        The {@link FailoverType} type that should be performed for given
     *                            test setup.
     * @param testDescriptor      The {@link TestDescriptor} used for test run.
     * @param failCheck           The {@link FailCheck} condition which is evaluated for every row
     *                            produced by source.
     * @param stream              The {@link DataStream} under test.
     * @param miniClusterResource The {@link MiniClusterWithClientResource} where given stream under
     *                            test is executed.
     * @return A {@link List} of produced records.
     * @implNote The {@code RecordCounterToFail::wrapWithFailureAfter} for every row checks the
     * "fail check" and if true and if this is a first fail check it completes the FAIL {@code
     * CompletableFuture} and waits on continueProcessing {@code CompletableFuture} next.
     * <p>
     * The flow is:
     * <ul>
     *      <li>
     *          The main test thread creates Flink's Streaming Environment.
     *      </li>
     *      <li>
     *          The main test thread creates Delta source.
     *      </li>
     *      <li>
     *          The main test thread wraps created source with {@code wrapWithFailureAfter} which
     *          has the {@code FailCheck} condition.
     *      </li>
     *      <li>
     *          The main test thread starts the "test Flink cluster" to produce records from
     *          Source via {@code DataStreamUtils.collectWithClient(...)}. As a result there is a
     *          Flink mini cluster created and data is consumed by source on a new thread.
     *      </li>
     *      <li>
     *          The main thread waits for "fail signal" that is issued by calling fail
     *          .complete. This is done on that new thread from point above. After calling {@code
     *          fail.complete} the source thread waits on {@code continueProcessing.get()};
     *       </li>
     *       <li>
     *           When the main thread sees that fail.complete was executed by the Source
     *          thread, it triggers the "generic" failover based on failoverType by calling
     *          {@code triggerFailover(...)}.
     *      </li>
     *      <li>
     *          After failover is complied, the main thread calls
     *          {@code RecordCounterToFail::continueProcessing},
     *          which releases the Source thread and resumes record consumption.
     *       </li>
     * </ul>
     * For test where FailoverType == NONE, we trigger fail signal on a first record, Main thread
     * executes triggerFailover method which only sends a continueProcessing signal that resumes
     * the Source thread.
     */
    public static <T> List<List<T>> testContinuousStream(
            FailoverType failoverType,
            TestDescriptor testDescriptor,
            FailCheck failCheck,
            DataStream<T> stream,
            MiniClusterWithClientResource miniClusterResource) throws Exception {

        DataStream<T> failingStreamDecorator =
            RecordCounterToFail.wrapWithFailureAfter(stream, failCheck);

        ClientAndIterator<T> client =
            DataStreamUtils.collectWithClient(failingStreamDecorator,
                "Continuous Delta Source Test");

        JobID jobId = client.client.getJobID();

        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

        // Read data from initial snapshot
        Future<List<T>> initialDataFuture =
            startInitialResultsFetcherThread(testDescriptor, client, singleThreadExecutor);

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(testDescriptor.getTablePath());

        // Read data from table updates.
        Future<List<T>> tableUpdaterFuture =
            startTableUpdaterThread(testDescriptor, tableUpdater, client, singleThreadExecutor);

        RecordCounterToFail.waitToFail();
        DeltaTestUtils.triggerFailover(
            failoverType,
            jobId,
            RecordCounterToFail::continueProcessing,
            miniClusterResource.getMiniCluster());

        // Main thread waits up to 5 minutes for all threads to finish. Fails of timeout.
        List<List<T>> totalResults = new ArrayList<>();
        totalResults.add(initialDataFuture.get(5, TimeUnit.MINUTES));
        totalResults.add(tableUpdaterFuture.get(5, TimeUnit.MINUTES));
        client.client.cancel().get(5, TimeUnit.MINUTES);

        return totalResults;
    }

    public static <T> Future<List<T>> startInitialResultsFetcherThread(
            TestDescriptor testDescriptor,
            ClientAndIterator<T> client,
            ExecutorService threadExecutor) {

        return threadExecutor.submit(
            () -> (DataStreamUtils.collectRecordsFromUnboundedStream(client,
                testDescriptor.getInitialDataSize())));
    }

    public static <T> Future<List<T>> startTableUpdaterThread(
            TestDescriptor testDescriptor,
            DeltaTableUpdater tableUpdater,
            ClientAndIterator<T> client,
            ExecutorService threadExecutor) {

        return threadExecutor.submit(
            () ->
            {
                List<T> results = new LinkedList<>();
                testDescriptor.getUpdateDescriptors().forEach(descriptor -> {
                    tableUpdater.writeToTable(descriptor);
                    List<T> records = DataStreamUtils.collectRecordsFromUnboundedStream(client,
                        descriptor.getNumberOfNewRows());
                    LOG.info("Stream update result size: " + records.size());
                    results.addAll(records);
                });
                return results;
            });
    }

    /**
     * Creates a {@link TestDescriptor} for tests. The descriptor created by this method
     * describes a scenario where Delta table will be updated
     * {@link TableUpdateDescriptor#getNumberOfNewVersions()}
     * times, where every update/version will contain
     * {@link TableUpdateDescriptor#getNumberOfRecordsPerNewVersion()}
     * new unique rows.
     */
    public static TestDescriptor prepareTableUpdates(
            String tablePath,
            RowType rowType,
            int initialDataSize,
            TableUpdateDescriptor tableUpdateDescriptor) {

        TestDescriptor testDescriptor =
            new TestDescriptor(tablePath, initialDataSize);

        for (int i = 0; i < tableUpdateDescriptor.getNumberOfNewVersions(); i++) {
            List<Row> newRows = new ArrayList<>();
            for (int j = 0; j < tableUpdateDescriptor.getNumberOfRecordsPerNewVersion(); j++) {
                newRows.add(Row.of("John-" + i + "-" + j, "Wick-" + i + "-" + j, j * i));
            }
            testDescriptor.add(rowType, newRows);
        }
        return testDescriptor;
    }

    /**
     * Reset Delta log file last modify timestamp value to current timestamp.
     * @param sourceTablePath table root folder
     * @throws IOException if the file could not otherwise be opened because it is not a directory.
     */
    public static void resetDeltaLogLastModifyTimestamp(String sourceTablePath) throws IOException {

        List<java.nio.file.Path> sortedLogFiles =
            Files.list(Paths.get(sourceTablePath + "/_delta_log"))
                .filter(file -> file.getFileName().toUri().toString().endsWith(".json"))
                .sorted()
                .collect(Collectors.toList());

        for (java.nio.file.Path logFile : sortedLogFiles) {
            assertThat(
                "Unable to modify " + logFile + " last modified timestamp.",
                logFile.toFile().setLastModified(System.currentTimeMillis()), equalTo(true));
        }
    }

    public static DynamicTableFactory.Context createTableContext(
        ResolvedSchema schema, Map<String, String> options) {

        return new FactoryUtil.DefaultDynamicTableContext(
            ObjectIdentifier.of("default", "default", "context_1"),
            new ResolvedCatalogTable(
                CatalogTable.of(
                    Schema.newBuilder().fromResolvedSchema(schema).build(),
                    "mock context",
                    Collections.emptyList(),
                    options
                ),
                schema
            ),
            Collections.emptyMap(),
            new Configuration(),
            DeltaTestUtils.class.getClassLoader(),
            false
        );
    }

    public static List<Integer> readParquetTable(String tablePath) throws Exception {
        try (Stream<java.nio.file.Path> stream = Files.list(Paths.get((tablePath)))) {
            Set<String> collect = stream
                .filter(file -> !Files.isDirectory(file))
                .map(java.nio.file.Path::getFileName)
                .map(java.nio.file.Path::toString)
                .filter(name -> !name.contains("inprogress"))
                .collect(Collectors.toSet());

            List<Integer> data = new ArrayList<>();
            for (String fileName : collect) {
                System.out.println(fileName);
                data.addAll(readParquetFile(
                        new Path(tablePath + fileName),
                        new org.apache.hadoop.conf.Configuration()
                    )
                );
            }
            return data;
        }
    }

    public static StreamExecutionEnvironment getTestStreamEnv(boolean streamingMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        if (streamingMode) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        return env;
    }

    /**
     * Verifies if Delta table under parameter {@code tablePath} contains expected number of rows
     * with given rowType format.
     *
     * @param tablePath               Path to Delta table.
     * @param rowType                 {@link RowType} for test Delta table.
     * @param expectedNumberOfRecords expected number of row in Delta table.
     * @return Head snapshot of Delta table.
     * @throws IOException If any issue while reading Delta Table.
     */
    @SuppressWarnings("unchecked")
    public static Snapshot verifyDeltaTable(
            String tablePath,
            RowType rowType,
            Integer expectedNumberOfRecords) throws IOException {

        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);
        Snapshot snapshot = deltaLog.snapshot();
        List<AddFile> deltaFiles = snapshot.getAllFiles();
        int finalTableRecordsCount = TestParquetReader
            .readAndValidateAllTableRecords(
                deltaLog,
                rowType,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(rowType)));
        long finalVersion = snapshot.getVersion();

        LOG.info(
            String.format(
                "RESULTS: final record count: [%d], final table version: [%d], number of Delta "
                    + "files: [%d]",
                finalTableRecordsCount,
                finalVersion,
                deltaFiles.size()
            )
        );

        assertThat(finalTableRecordsCount, equalTo(expectedNumberOfRecords));
        return snapshot;
    }

    private static List<Integer> readParquetFile(
            Path filePath,
            org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {

        ParquetFileReader reader = ParquetFileReader.open(
            HadoopInputFile.fromPath(filePath, hadoopConf)
        );

        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;

        List<Integer> data = new LinkedList<>();
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader recordReader =
                columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                data.add(simpleGroup.getInteger("age", 0));
            }
        }
        Collections.sort(data);
        return data;
    }

    /**
     * Creates a Delta table with single Metadata action containing table properties passed via
     * configuration argument or adds table properties to existing Delta table.
     *
     * @param tablePath     path where which Delta table will be created.
     * @param configuration table properties that will be added to Delta table.
     * @return a {@link DeltaLog} instance for created Delta table.
     */
    public static DeltaLog setupDeltaTableWithProperties(
            String tablePath,
            Map<String, String> configuration) {
        return setupDeltaTable(tablePath, configuration, null);
    }

    /**
     * Creates a Delta table with single Metadata action containing table properties and schema
     * passed via `configuration` and `schema` parameters. If Delta table exists under specified
     * tablePath properties and schema will be committed to existing Delta table as new
     * transaction.
     * <p>
     * The `schema` argument can be null if Delta table exists under `tablePath`. In that case only
     * table properties from `configuration` parameter will be added to the Delta table.
     *
     * @param tablePath     path for Delta table.
     * @param configuration configuration with table properties that should be added to Delta
     *                      table.
     * @param metadata      metadata for Delta table. Can be null if Delta table already exists.
     * @return {@link DeltaLog} instance for created or updated Delta table.
     */
    public static DeltaLog setupDeltaTable(
            String tablePath,
            Map<String, String> configuration,
            Metadata metadata) {

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        if (!deltaLog.tableExists() && (metadata == null || metadata.getSchema() == null)) {
            throw new IllegalArgumentException(
                String.format(
                    "Schema cannot be null/empty if table does not exists under given path %s",
                    tablePath));
        }

        // Set delta table property. DDL will try to override it with different value
        OptimisticTransaction transaction = deltaLog.startTransaction();
        Builder newMetadataBuilder = transaction.metadata()
            .copyBuilder()
            .configuration(configuration);

        if (metadata != null) {
            newMetadataBuilder
                .schema(metadata.getSchema())
                .partitionColumns(metadata.getPartitionColumns());
        }

        Metadata updatedMetadata = newMetadataBuilder
            .build();

        transaction.updateMetadata(updatedMetadata);
        transaction.commit(
            Collections.singletonList(updatedMetadata),
            new Operation((deltaLog.tableExists()) ? Name.SET_TABLE_PROPERTIES : Name.CREATE_TABLE),
            ConnectorUtils.ENGINE_INFO
        );
        return deltaLog;
    }

    public static MiniClusterResourceConfiguration buildClusterResourceConfig(
        int parallelismLevel) {

        Configuration configuration = new Configuration();

        // By default, let's check for leaked classes in tests.
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);

        return new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(parallelismLevel)
            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
            .withHaLeadershipControl()
            .setConfiguration(configuration)
            .build();
    }

    /**
     * Changes last modification time for delta log .json files.
     *
     * @param sourceTablePath        Path to delta log to change last modification time.
     * @param lastModifiedTimestamps An array of times to which _delta_log .json files last
     *                               modification time should be change to. If bigger than number of
     *                               .json files under _delta_log, an exception will be thrown.
     */
    public static void changeDeltaLogLastModifyTimestamp(
            String sourceTablePath,
            String[] lastModifiedTimestamps) throws IOException {

        List<java.nio.file.Path> sortedLogFiles =
            Files.list(Paths.get(sourceTablePath + "/_delta_log"))
                .filter(file -> file.getFileName().toUri().toString().endsWith(".json"))
                .sorted()
                .collect(Collectors.toList());

        if (lastModifiedTimestamps.length > sortedLogFiles.size()) {
            throw new IllegalArgumentException(String.format(""
                    + "Delta log for table %s size, does not match"
                    + " test's last modify argument size %d",
                sourceTablePath, lastModifiedTimestamps.length
            ));
        }

        int i = 0;
        for (java.nio.file.Path logFile : sortedLogFiles) {
            if (i >= lastModifiedTimestamps.length) {
                break;
            }
            String timestampAsOfValue = lastModifiedTimestamps[i++];
            long toTimestamp = TimestampFormatConverter.convertToTimestamp(timestampAsOfValue);
            LOG.info(
                "Changing Last Modified timestamp on file " + logFile
                    + " to " + timestampAsOfValue + " -> " + timestampAsOfValue
            );
            assertThat(
                "Unable to modify " + logFile + " last modified timestamp.",
                logFile.toFile().setLastModified(toTimestamp), equalTo(true));
        }
    }

    public static List<Row> readTableResult(TableResult tableResult) throws Exception {
        List<Row> resultData = new ArrayList<>();
        try(CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                resultData.add(collect.next());
            }
        }
        return resultData;
    }
}
