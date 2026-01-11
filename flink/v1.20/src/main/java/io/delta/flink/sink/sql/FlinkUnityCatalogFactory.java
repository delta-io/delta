/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.sql;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class FlinkUnityCatalogFactory implements CatalogFactory {

  public static final String IDENTIFIER = "unitycatalog";

  public static final ConfigOption<String> ENDPOINT =
      ConfigOptions.key("endpoint")
          .stringType()
          .noDefaultValue()
          .withDescription("REST endpoint of UnityCatalog");

  public static final ConfigOption<String> TOKEN =
      ConfigOptions.key("token")
          .stringType()
          .noDefaultValue()
          .withDescription("REST token of UnityCatalog");

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key("default-database").stringType().defaultValue("default");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    // Keep empty for minimal demo, but usually you’ll require endpoint/warehouse/etc.
    return Set.of(ENDPOINT, TOKEN);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(DEFAULT_DATABASE);
  }

  @Override
  public Catalog createCatalog(Context context) {
    // Validates options and provides convenient access:
    FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);

    helper.validate(); // validates required/optional + unknown options

    String endpoint = helper.getOptions().get(ENDPOINT);
    String token = helper.getOptions().get(TOKEN);
    String defaultDb = helper.getOptions().get(DEFAULT_DATABASE);
    return new FlinkUnityCatalog(context.getName(), defaultDb, endpoint, token);
  }
}
