package io.delta.table;

import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.ScanImpl;
import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.Pair;

class DeltaTableScan extends SimpleScan<DeltaTableScan> {
    private final Table deltaTable;
    private final Engine deltaEngine;

    DeltaTableScan(DeltaTable table, Table deltaTable, Engine deltaEngine) {
        super(table);
        this.deltaTable = deltaTable;
        this.deltaEngine = deltaEngine;
    }

    protected DeltaTableScan(DeltaTableScan toCopy) {
        super(toCopy);
        this.deltaTable = toCopy.deltaTable;
        this.deltaEngine = toCopy.deltaEngine;
    }

    @Override
    protected DeltaTableScan self() {
        return this;
    }

    @Override
    protected DeltaTableScan copy(DeltaTableScan toCopy) {
        return new DeltaTableScan(this);
    }

    @Override
    public CloseableIterable<ScanTask> planFiles() {
        Schema schema = schema();
        PartitionSpec spec = table().spec();
        Snapshot deltaSnapshot = ((DeltaSnapshot) snapshot()).deltaSnapshot();

        Predicate deltaFilter =
                DeltaExpressionUtil.convert(Binder.bind(schema.asStruct(), filter(), isCaseSensitive()));

        ScanImpl scan =
                (ScanImpl)
                        deltaSnapshot.getScanBuilder(deltaEngine).withFilter(deltaEngine, deltaFilter).build();

        String schemaString = SchemaParser.toJson(schema);
        String specString = PartitionSpecParser.toJson(spec);
        ResidualEvaluator residualEval = ResidualEvaluator.of(spec, filter(), isCaseSensitive());

        CloseableIterable<FilteredColumnarBatch> batches =
                fromLambda(() -> scan.getScanFiles(deltaEngine, true));

        CloseableIterable<CloseableIterable<Pair<DataFile, DeleteFile>>> fileBatches =
                CloseableIterable.transform(
                        batches, batch -> DeltaFileUtil.files(table().location(), schema, spec, batch));

        CloseableIterable<Pair<DataFile, DeleteFile>> files = CloseableIterable.concat(fileBatches);

        return CloseableIterable.combine(
                DeltaFileUtil.asTasks(schemaString, specString, residualEval, files), batches);
    }

    static <E, C extends Iterator<E> & Closeable> CloseableIterable<E> fromLambda(
            Supplier<C> newIterator) {
        return new LambdaGroup<>(newIterator);
    }

    static class LambdaGroup<E, C extends Iterator<E> & Closeable> extends CloseableGroup
            implements CloseableIterable<E> {
        private final Supplier<C> supplier;

        public LambdaGroup(Supplier<C> supplier) {
            this.supplier = supplier;
        }

        @Override
        public CloseableIterator<E> iterator() {
            C iter = supplier.get();
            addCloseable(iter);
            return CloseableIterator.withClose(iter);
        }
    }
}
