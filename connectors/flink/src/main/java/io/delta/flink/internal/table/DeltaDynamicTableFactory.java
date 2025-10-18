/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.logical.RowType;

/**
 * Creates a {@link DynamicTableSink} and {@link DynamicTableSource} instance representing DeltaLake
 * table.
 *
 * <p>
 * This implementation automatically resolves all necessary object for creating instance of {@link
 * io.delta.flink.sink.DeltaSink} and {@link io.delta.flink.source.DeltaSource} except Delta table's
 * path that needs to be provided explicitly.
 */
public class DeltaDynamicTableFactory implements DynamicTableSinkFactory,
        DynamicTableSourceFactory {

    public static final String DELTA_CONNECTOR_IDENTIFIER = "delta";

    /**
     * Flag that will be set to true only when creating Delta Table Factory from Delta Catalog. If
     * this flag is set to false, then createDynamicTableSink and createDynamicTableSource methods
     * will throw an exception when called.
     */
    public final boolean isFromCatalog;

    /**
     * This constructor is meant to be use by Flink Factory discovery mechanism. This constructor
     * will set "fromCatalog" field to false which will make createDynamicTableSink and
     * createDynamicTableSource methods throw an exception informing that Flink SQL support for
     * Delta tables must be used with Delta Catalog only.
     *
     * @implNote In order to support
     * {@link org.apache.flink.table.api.bridge.java.StreamTableEnvironment}, factory must not throw
     * exception from constructor. The StreamTableEnvironment when initialized loads and cache all
     * Factories defined in META-INF/service/org.apache.flink.table.factories.Factory file before
     * executing any SQL/Table call.
     */
    public DeltaDynamicTableFactory() {
        this.isFromCatalog = false;
    }

    private DeltaDynamicTableFactory(boolean isFromCatalog) {
        if (!isFromCatalog) {
            throw new RuntimeException("FromCatalog parameter must be set to true.");
        }

        this.isFromCatalog = true;
    }

    static DeltaDynamicTableFactory fromCatalog() {
        return new DeltaDynamicTableFactory(true);
    }

    @Override
    public String factoryIdentifier() {
        return DELTA_CONNECTOR_IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        if (!isFromCatalog) {
            throw notFromDeltaCatalogException();
        }

        // Check if requested table is Delta or not.
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        org.apache.flink.configuration.Configuration options = getQueryOptions(helper);

        String connectorType = options.get(FactoryUtil.CONNECTOR);
        if (!DELTA_CONNECTOR_IDENTIFIER.equals(connectorType)) {

            // Look for Table factory proper fort this table type.
            DynamicTableSinkFactory sinkFactory =
                FactoryUtil.discoverFactory(this.getClass().getClassLoader(),
                    DynamicTableSinkFactory.class, connectorType);
            return sinkFactory.createDynamicTableSink(context);
        }

        // This must have been a Delta Table, so continue with this factory
        DeltaTableFactoryHelper.validateSinkQueryOptions(options);

        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();

        org.apache.hadoop.conf.Configuration conf =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());

        RowType rowType = (RowType) tableSchema.toSinkRowDataType().getLogicalType();

        return new DeltaDynamicTableSink(
            new Path(options.get(DeltaTableConnectorOptions.TABLE_PATH)),
            conf,
            rowType,
            context.getCatalogTable()
        );
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        if (!isFromCatalog) {
            throw notFromDeltaCatalogException();
        }

        // Check if requested table is Delta or not.
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        org.apache.flink.configuration.Configuration options = getQueryOptions(helper);

        String connectorType = options.get(FactoryUtil.CONNECTOR);
        if (!DELTA_CONNECTOR_IDENTIFIER.equals(connectorType)) {

            // Look for Table factory proper fort this table type.
            DynamicTableSourceFactory sourceFactory =
                FactoryUtil.discoverFactory(this.getClass().getClassLoader(),
                    DynamicTableSourceFactory.class, connectorType);
            return sourceFactory.createDynamicTableSource(context);
        }

        // This must have been a Delta Table, so continue with this factory
        QueryOptions queryOptions = DeltaTableFactoryHelper.validateSourceQueryOptions(options);

        org.apache.hadoop.conf.Configuration hadoopConf =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());

        List<String> columns = ((RowType) context
            .getCatalogTable()
            .getResolvedSchema()
            .toPhysicalRowDataType()
            .getLogicalType()
        ).getFieldNames();

        return new DeltaDynamicTableSource(
            hadoopConf,
            queryOptions,
            columns
        );
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.TABLE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // We do not use Flink's helper validation logic. We are using our own instead.
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // We do not use Flink's helper validation logic. We are using our own instead.
        return Collections.emptySet();
    }

    private RuntimeException notFromDeltaCatalogException() {
        return new RuntimeException("Delta Table SQL/Table API was used without Delta Catalog. "
            + "It is required to use Delta Catalog with all Flink SQL operations that involve "
            + "Delta table. Please see documentation for details -> TODO DC add link to docs");
    }

    private Configuration getQueryOptions(TableFactoryHelper helper) {
        // This cast is generally safe because FactoryUtil.TableFactoryHelper::getOptions()
        // will always return an instance of org.apache.flink.configuration.Configuration.
        // The Configuration type we are casting to, provide 'toMap()' method that allows us to get
        // all properties provided by Catalog and Query Hints to the factory.
        // The original 'ReadableConfig' type provide only `get(propertyName)` method.
        // By doing this cast we can validate and throw an exception if any none allowed options
        // were used. Without this cast, we could only verify known properties and silently ignore
        // unknown ones.
        return (Configuration) helper.getOptions();
    }
}
