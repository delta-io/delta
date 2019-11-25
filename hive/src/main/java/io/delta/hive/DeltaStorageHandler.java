package io.delta.hive;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.collection.JavaConverters;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.delta.DeltaHelper;
import org.apache.spark.sql.delta.DeltaPushFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaStorageHandler extends DefaultStorageHandler
    implements HiveMetaHook, HiveStoragePredicateHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaStorageHandler.class);

    public static final String DELTA_TABLE_PATH = "delta.table.path";
    public static final String DELTA_PARTITION_COLS_NAMES = "delta.partition.columns";
    public static final String DELTA_PARTITION_COLS_TYPES = "delta.partition.columns.types";

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return DeltaInputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return ParquetHiveSerDe.class;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        super.configureInputJobProperties(tableDesc, jobProperties);
        jobProperties.put(DELTA_TABLE_PATH, tableDesc.getProperties().getProperty(DELTA_TABLE_PATH));
        jobProperties.put(DELTA_PARTITION_COLS_NAMES, tableDesc.getProperties().getProperty(DELTA_PARTITION_COLS_NAMES));
        jobProperties.put(DELTA_PARTITION_COLS_TYPES, tableDesc.getProperties().getProperty(DELTA_PARTITION_COLS_TYPES));
    }

    @Override
    public DecomposedPredicate decomposePredicate(
            JobConf jobConf, Deserializer deserializer,
            ExprNodeDesc predicate) {
        // Get the delta root path
        String deltaRootPath = jobConf.get(DELTA_TABLE_PATH);
        // Get the partitionColumns of Delta
        List<String> partitionColumns = JavaConverters.seqAsJavaList(
                DeltaHelper.getPartitionCols(new Path(deltaRootPath)));
        LOG.info("delta partitionColumns is " + Joiner.on(",").join(partitionColumns));

        IndexPredicateAnalyzer analyzer = newIndexPredicateAnalyzer(partitionColumns);

        List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
        ExprNodeGenericFuncDesc pushedPredicate = null;
        ExprNodeGenericFuncDesc residualPredicate =
                (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(predicate, conditions);

        for (List<IndexSearchCondition> searchConditions : decompose(conditions).values()) {
            // still push back the pushedPredicate to residualPredicate
            residualPredicate =
                    extractResidualCondition(analyzer, searchConditions, residualPredicate);
            pushedPredicate =
                    extractStorageHandlerCondition(analyzer, searchConditions, pushedPredicate);
        }

        LOG.info("pushedPredicate:" + (pushedPredicate == null? "null":pushedPredicate.getExprString())
            + ",residualPredicate" + residualPredicate);

        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = pushedPredicate;
        decomposedPredicate.residualPredicate = residualPredicate;
        return decomposedPredicate;
    }

    private IndexPredicateAnalyzer newIndexPredicateAnalyzer(
            List<String> partitionColumns) {
        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        for (String col : partitionColumns) {
            // Supported filter exprs on partition column to be pushed down to delta
            analyzer.addComparisonOp(col, DeltaPushFilter.supportedPushDownUDFs());
        }
        return analyzer;
    }

    private static Map<String, List<IndexSearchCondition>> decompose(
            List<IndexSearchCondition> searchConditions) {
        Map<String, List<IndexSearchCondition>> result =
                new HashMap<String, List<IndexSearchCondition>>();
        for (IndexSearchCondition condition : searchConditions) {
            List<IndexSearchCondition> conditions = result.get(condition.getColumnDesc().getColumn());
            if (conditions == null) {
                conditions = new ArrayList<IndexSearchCondition>();
                result.put(condition.getColumnDesc().getColumn(), conditions);
            }
            conditions.add(condition);
        }
        return result;
    }

    private static ExprNodeGenericFuncDesc extractResidualCondition(
            IndexPredicateAnalyzer analyzer,
            List<IndexSearchCondition> searchConditions, ExprNodeGenericFuncDesc inputExpr) {
        if (inputExpr == null) {
            return analyzer.translateOriginalConditions(searchConditions);
        }
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(analyzer.translateOriginalConditions(searchConditions));
        children.add(inputExpr);
        return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                FunctionRegistry.getGenericUDFForAnd(), children);
    }

    private static ExprNodeGenericFuncDesc extractStorageHandlerCondition(
            IndexPredicateAnalyzer analyzer,
            List<IndexSearchCondition> searchConditions, ExprNodeGenericFuncDesc inputExpr) {
        if (inputExpr == null) {
            return analyzer.translateSearchConditions(searchConditions);
        }
        List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        children.add(analyzer.translateSearchConditions(searchConditions));
        children.add(inputExpr);
        return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                FunctionRegistry.getGenericUDFForAnd(), children);
    }

    public HiveMetaHook getMetaHook() {
        return this;
    }

    public void preCreateTable(Table tbl) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
        if (!isExternal) {
            throw new MetaException("HiveOnDelta should be an external table.");
        } else if (tbl.getPartitionKeysSize() > 0) {
            throw new MetaException("HiveOnDelta does not support to create a partition hive table");
        }

        String deltaRootString = tbl.getSd().getLocation();
        try {
            if (deltaRootString == null || deltaRootString.trim().length() == 0) {
                throw new MetaException("table location should be set when creating table");
            } else {
                Path deltaPath = new Path(deltaRootString);
                FileSystem fs = deltaPath.getFileSystem(getConf());
                if (!fs.exists(deltaPath)) {
                    throw new MetaException("delta.table.path(" + deltaRootString + ") does not exist...");
                } else {
                    Map<String, String> partitionProps = JavaConverters.mapAsJavaMap(
                            DeltaHelper.checkHiveColsInDelta(deltaPath, tbl.getSd().getCols()));
                    tbl.getSd().getSerdeInfo().getParameters().putAll(partitionProps);
                    tbl.getSd().getSerdeInfo().getParameters().put(DELTA_TABLE_PATH, deltaRootString);
                    LOG.info("write partition cols/types to table properties " +
                            Joiner.on(",").withKeyValueSeparator("=").join(partitionProps));
                }
            }
        } catch (Exception e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    public void rollbackCreateTable(Table table) throws MetaException {
        // nothing to do
    }

    public void commitCreateTable(Table table) throws MetaException {
        // nothing to do
    }

    public void preDropTable(Table table) throws MetaException {
        // nothing to do
    }

    public void rollbackDropTable(Table table) throws MetaException {
        // nothing to do
    }

    public void commitDropTable(Table tbl, boolean deleteData) throws MetaException {
        // nothing to do
    }
}
