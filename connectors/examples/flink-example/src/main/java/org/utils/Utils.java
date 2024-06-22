package org.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public final class Utils {

    static int PRINT_PAD_LENGTH = 4;

    private Utils() {}

    public static final RowType FULL_SCHEMA_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("f1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f3", new IntType())
    ));

    public static String resolveExampleTableAbsolutePath(String resourcesTableDir) {
        String rootPath = Paths.get(".").toAbsolutePath().normalize().toString();
        return rootPath.endsWith("flink-example") ?
            // Maven commands are run from the examples/flink-example/ directory
            rootPath + "/src/main/resources/" + resourcesTableDir :
            // while SBT commands are run from the examples/ directory
            rootPath + "/examples/flink-example/src/main/resources/" + resourcesTableDir;
    }

    public static void prepareDirs(String tablePath) throws IOException {
        File tableDir = new File(tablePath);
        if (tableDir.exists()) {
            FileUtils.cleanDirectory(tableDir);
        } else {
            tableDir.mkdirs();
        }
    }

    public static void prepareDirs(String sourcePath, String workPath) throws IOException {
        prepareDirs(workPath);
        System.out.printf("Copy example table data from %s to %s%n%n", sourcePath, workPath);
        FileUtils.copyDirectory(new File(sourcePath), new File(workPath));
    }

    public static ScheduledFuture<?> runSourceTableUpdater(String tablePath) {

        final DeltaTableUpdater tableUpdater = new DeltaTableUpdater(tablePath);

        AtomicInteger index = new AtomicInteger(0);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        return scheduler.scheduleWithFixedDelay(
            () -> {
                int i = index.getAndIncrement();
                List<Row> rows = Collections.singletonList(
                    Row.of("f1_newVal_" + i, "f2_newVal_" + i, i));
                Descriptor descriptor = new Descriptor(tablePath, Utils.FULL_SCHEMA_ROW_TYPE, rows);
                tableUpdater.writeToTable(descriptor);
            },
            10,
            2,
            TimeUnit.SECONDS
        );
    }

    public static void printDeltaTableRows(String tablePath) throws InterruptedException {
        DeltaLog deltaLog =
            DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(), tablePath);

        for (int i = 0; i < 30; i++) {
            deltaLog.update();
            Snapshot snapshot = deltaLog.snapshot();

            System.out.println("===== current snapshot =====");
            System.out.println("snapshot version: " + snapshot.getVersion());
            System.out.println("number of total data files: " + snapshot.getAllFiles().size());

            CloseableIterator<RowRecord> iter = snapshot.open();
            System.out.println("\ntable rows:");
            System.out.println(StringUtils.rightPad("f1", PRINT_PAD_LENGTH) + "| " +
                StringUtils.rightPad("f2", PRINT_PAD_LENGTH) + " | " +
                StringUtils.rightPad("f3", PRINT_PAD_LENGTH));
            System.out.println(String.join("", Collections.nCopies(4 * PRINT_PAD_LENGTH, "-")));

            RowRecord row = null;
            int numRows = 0;
            while (iter.hasNext()) {
                row = iter.next();
                numRows++;

                String f1 = row.isNullAt("f1") ? null : row.getString("f1");
                String f2 = row.isNullAt("f2") ? null : row.getString("f2");
                Integer f3 = row.isNullAt("f3") ? null : row.getInt("f3");

                System.out.println(StringUtils.rightPad(f1, PRINT_PAD_LENGTH) + "| " +
                    StringUtils.rightPad(f2, PRINT_PAD_LENGTH) + " | " +
                    StringUtils.rightPad(String.valueOf(f3), PRINT_PAD_LENGTH));
            }
            System.out.println("\nnumber rows: " + numRows);
            if (row != null) {
                System.out.println("data schema:");
                System.out.println(row.getSchema().getTreeString());
                System.out.println("partition cols:");
                System.out.println(snapshot.getMetadata().getPartitionColumns());
            }
            System.out.println("\n");
            Thread.sleep(5000);
        }
    }
}
