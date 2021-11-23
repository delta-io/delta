package io.delta.standalone.example;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Demonstrates how the Delta Standalone library can be used to convert a parquet table
 * (i.e., a directory of parquet files) into a Delta table by writing the list of parquet files
 * as a Delta log in the directory.
 *
 * To generate your own parquet files for the example, see resources/generateParquet.py
 *
 * To run this example:
 * - cd connectors/examples/standalone-example-convert-to-delta
 * - mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=io.delta.standalone.example.ConvertToDelta
 *
 * Find the converted table in: target/classes/$targetTable
 */
public class ConvertToDelta {

    private static void convertToDelta(Path sourcePath, Path targetPath,
            StructType sourceSchema) throws IOException {

        Configuration conf = new Configuration();
        DeltaLog log = DeltaLog.forTable(conf, targetPath);

        if (log.snapshot().getVersion() > -1) {
            // there is already a non-empty targetPath/_delta_log
            System.out.println("The table you are trying to convert is already a delta table");
            return;
        }

        // ---------------------- Generate commit actions ------------------------

        if (DeltaLog.forTable(conf, sourcePath).snapshot().getVersion() > -1) {
            // the parquet data files are already part of a delta table
            System.out.println("The table you are trying to convert is already a delta table");
            return;
        }
        FileSystem fs = sourcePath.getFileSystem(conf);

        // find parquet files
        List<FileStatus> files = Arrays.stream(fs.listStatus(sourcePath))
                .filter(f -> f.isFile() && f.getPath().getName().endsWith(".parquet"))
                .collect(Collectors.toList());

        // generate AddFiles
        List<AddFile> addFiles = files.stream().map(file -> {
            return new AddFile(
                    // if targetPath is not a prefix, relativize returns the path unchanged
                    targetPath.toUri().relativize(file.getPath().toUri()).toString(),   // path
                    Collections.emptyMap(),                                             // partitionValues
                    file.getLen(),                                                      // size
                    file.getModificationTime(),                                         // modificationTime
                    true,                                                               // dataChange
                    null,                                                               // stats
                    null                                                                // tags
            );
        }).collect(Collectors.toList());

        Metadata metadata = Metadata.builder().schema(sourceSchema).build();

        // ---------------------- Commit to Delta log --------------------------

        OptimisticTransaction txn = log.startTransaction();
        txn.updateMetadata(metadata);
        txn.commit(addFiles, new Operation(Operation.Name.CONVERT), "local");
    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        // ---------------------- User configuration (input) ----------------------

        final String sourceTable = "external/sales";

        final String targetTable = "external/sales";

        final StructType sourceSchema = new StructType()
                .add("year", new IntegerType())
                .add("month", new IntegerType())
                .add("day", new IntegerType())
                .add("sale_id", new StringType())
                .add("customer", new StringType())
                .add("total_cost", new FloatType());

        // ---------------------- Internal file system configuration ----------------------

        // look for target table
        URL targetURL = ConvertToDelta.class.getClassLoader().getResource(targetTable);
        if (targetURL == null) {
            // target directory does not exist, create it (relative to package location)
            java.nio.file.Path rootPath = Paths.get(ConvertToDelta.class.getResource("/").toURI());
            FileUtils.forceMkdir(new File(rootPath.toFile(), targetTable));
        }

        final Path sourcePath = new Path(ConvertToDelta.class.getClassLoader().getResource(sourceTable).toURI());
        final Path targetPath = new Path(ConvertToDelta.class.getClassLoader().getResource(targetTable).toURI());

        // -------------------------- Convert table to Delta ---------------------------

        convertToDelta(sourcePath, targetPath, sourceSchema);

        // ---------------------------- Verify conversion ----------------------------------

        // read from Delta Log
        DeltaLog log = DeltaLog.forTable(new Configuration(), targetPath);
        Snapshot currentSnapshot = log.snapshot();
        StructType schema = currentSnapshot.getMetadata().getSchema();

        System.out.println("current version: " + currentSnapshot.getVersion());

        System.out.println("number data files: " + currentSnapshot.getAllFiles().size());

        System.out.println("data files:");
        CloseableIterator<AddFile> dataFiles = currentSnapshot.scan().getFiles();
        dataFiles.forEachRemaining(file -> System.out.println(file.getPath()));
        dataFiles.close();

        System.out.println("schema: ");
        System.out.println(schema.getTreeString());

        System.out.println("first 5 rows:");
        CloseableIterator<RowRecord> iter = currentSnapshot.open();
        try {
            int i = 0;
            while (iter.hasNext() && i < 5) {
                i++;
                RowRecord row = iter.next();
                int year = row.isNullAt("year") ? null : row.getInt("year");
                int month = row.isNullAt("month") ? null : row.getInt("month");
                int day = row.isNullAt("day") ? null : row.getInt("day");
                String sale_id = row.isNullAt("sale_id") ? null : row.getString("sale_id");
                String customer = row.isNullAt("customer") ? null : row.getString("customer");
                float total_cost = row.isNullAt("total_cost") ? null : row.getFloat("total_cost");
                System.out.println(year + " " + month + " " + day + " " + sale_id + " " + customer + " " + total_cost);
            }
        } finally {
            iter.close();
        }
    }
}
