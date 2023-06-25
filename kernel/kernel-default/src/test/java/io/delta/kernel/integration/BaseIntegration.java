package io.delta.kernel.integration;

import static io.delta.kernel.data.vector.VectorUtils.getValueAsObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.client.DefaultTableClient;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Base class containing utility method to write integration tests that read data from
 * Delta tables using the Kernel APIs.
 */
public abstract class BaseIntegration
{
    protected TableClient tableClient = DefaultTableClient.create(new Configuration());

    protected Table table(String path) throws Exception
    {
        return Table.forPath(path);
    }

    protected Snapshot snapshot(String path) throws Exception
    {
        return table(path).getLatestSnapshot(tableClient);
    }

    protected List<ColumnarBatch> readSnapshot(StructType readSchema, Snapshot snapshot)
        throws Exception
    {
        Scan scan = snapshot.getScanBuilder(tableClient)
            .withReadSchema(tableClient, readSchema)
            .build();

        Row scanState = scan.getScanState(tableClient);
        CloseableIterator<ColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);

        return readScanFiles(scanState, scanFileIter);
    }

    protected List<ColumnarBatch> readScanFiles(
        Row scanState,
        CloseableIterator<ColumnarBatch> scanFilesBatchIter) throws Exception
    {
        List<ColumnarBatch> dataBatches = new ArrayList<>();
        try {
            while (scanFilesBatchIter.hasNext()) {
                // Read data
                try (CloseableIterator<DataReadResult> data =
                    Scan.readData(
                        tableClient,
                        scanState,
                        scanFilesBatchIter.next().getRows(),
                        Optional.empty())) {
                    while (data.hasNext()) {
                        DataReadResult dataReadResult = data.next();
                        assertFalse(dataReadResult.getSelectionVector().isPresent());
                        dataBatches.add(dataReadResult.getData());
                    }
                }
            }
        }
        finally {
            scanFilesBatchIter.close();
        }

        return dataBatches;
    }

    /**
     * Remove unsupported top level delta types in Kernel from the schema. Unsupported data types
     * include `DECIMAL` and `TIMESTAMP`.
     */
    protected StructType removeUnsupportedType(StructType schema)
    {
        List<StructField> filterList =
            schema.fields().stream().filter(
                field -> !(field.getDataType() instanceof DecimalType ||
                    field.getDataType() instanceof TimestampType)
            ).collect(Collectors.toList());

        return new StructType(filterList);
    }

    protected void compareEqualUnorderd(ColumnarBatch expDataBatch,
        List<ColumnarBatch> actDataBatches)
    {
        Set<Integer> expDataRowsMatched = new HashSet<>();
        for (int actDataBatchIdx = 0; actDataBatchIdx < actDataBatches.size(); actDataBatchIdx++) {
            ColumnarBatch actDataBatch = actDataBatches.get(actDataBatchIdx);

            assertEquals(expDataBatch.getSchema(), actDataBatch.getSchema());

            for (int actRowIdx = 0; actRowIdx < actDataBatch.getSize(); actRowIdx++) {
                boolean matched = false;
                for (int expRowIdx = 0; expRowIdx < expDataBatch.getSize(); expRowIdx++) {
                    // If the row is already matched by another record, don't match again
                    if (expDataRowsMatched.contains(expRowIdx)) {
                        continue;
                    }

                    matched = compareRows(expDataBatch, expRowIdx, actDataBatch, actRowIdx);
                    if (matched) {
                        expDataRowsMatched.add(expRowIdx);
                        break;
                    }
                }
                // TODO: improve the logging info
                assertTrue("Actual data contain a row that is not expected", matched);
            }
        }

        // TODO: improve the logging info
        assertEquals(
            "An expected row is not present in the actual data output",
            expDataBatch.getSize(),
            expDataRowsMatched.size());
    }

    protected boolean compareRows(
        ColumnarBatch expDataBatch,
        int expRowId,
        ColumnarBatch actDataBatch,
        int actRowId)
    {
        StructType readSchema = expDataBatch.getSchema();

        for (int fieldId = 0; fieldId < readSchema.length(); fieldId++) {
            ColumnVector expDataVector = expDataBatch.getColumnVector(fieldId);
            ColumnVector actDataVector = actDataBatch.getColumnVector(fieldId);

            Object expObject = getValueAsObject(expDataVector, expRowId);
            Object actObject = getValueAsObject(actDataVector, actRowId);
            boolean matched = Objects.deepEquals(expObject, actObject);
            if (!matched) {
                return false;
            }
        }

        return true;
    }
}
