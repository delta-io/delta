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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.*;
import org.apache.flink.table.types.logical.*;

public class FlinkUnityCatalogTable implements CatalogTable {

  private final String name;
  private final String comment;
  private final String description;
  private final Map<String, String> properties;
  private final Schema schema;
  private final String dataFormat;

  private final URI endpoint;
  private final String token;

  public FlinkUnityCatalogTable(
      String name,
      Schema schema,
      String dataFormat,
      String comment,
      String description,
      Map<String, String> properties,
      URI endpoint,
      String token) {
    this.name = name;
    this.dataFormat = dataFormat;
    this.comment = comment;
    this.description = description;
    this.properties = properties;
    this.schema = schema;

    this.endpoint = endpoint;
    this.token = token;
  }

  public FlinkUnityCatalogTable(TableInfo tableInfo, URI endpoint, String token) {
    this(
        tableInfo.getName(),
        buildSchema(tableInfo.getColumns()),
        Optional.ofNullable(tableInfo.getDataSourceFormat())
            .map(DataSourceFormat::getValue)
            .orElse("UNKNOWN"),
        tableInfo.getComment(),
        "",
        tableInfo.getProperties(),
        endpoint,
        token);
  }

  public static Schema buildSchema(List<ColumnInfo> columnInfos) {
    Schema.Builder builder = Schema.newBuilder();
    columnInfos.forEach(
        colinfo ->
            builder
                .column(colinfo.getName(), fromJson(colinfo.getTypeJson()))
                .withComment(colinfo.getComment()));
    return builder.build();
  }

  static Pattern decimalPattern = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");

  public static DataType fromJson(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(json);
      boolean nullable = root.get("nullable").asBoolean();
      JsonNode rootType = root.get("type");
      if (rootType.isObject()) {
        switch (rootType.get("type").asText()) {
          case "struct":
            JsonNode fields = rootType.get("fields");
            List<DataType> dataTypes = new ArrayList<>();
            List<RowType.RowField> rowFields = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
              JsonNode field = fields.get(i);
              String fieldName = field.get("name").asText();
              DataType fieldType = fromJson(field.toString());
              rowFields.add(new RowType.RowField(fieldName, fieldType.getLogicalType()));
              dataTypes.add(fieldType);
            }
            return new FieldsDataType(new RowType(nullable, rowFields), dataTypes);
          case "array":
            boolean elementNullable = rootType.get("containsNull").asBoolean();
            String elementTypeString = rootType.get("elementType").toString();
            DataType elementType =
                fromJson(
                    String.format(
                        "{\"type\":%s, \"nullable\": \"%s\"}", elementTypeString, elementNullable));
            return new CollectionDataType(
                new ArrayType(
                    elementType.getLogicalType().isNullable(), elementType.getLogicalType()),
                elementType);
          case "map":
            DataType keyType =
                fromJson(
                    String.format(
                        "{\"type\":%s, \"nullable\": \"%s\"}", rootType.get("keyType"), false));
            DataType valueType =
                fromJson(
                    String.format(
                        "{\"type\":%s, \"nullable\": \"%s\"}",
                        rootType.get("valueType"), rootType.get("valueContainsNull").asBoolean()));
            return new KeyValueDataType(
                new MapType(
                    valueType.getLogicalType().isNullable(),
                    keyType.getLogicalType(),
                    valueType.getLogicalType()),
                keyType,
                valueType);
          default:
            throw new UnsupportedOperationException("Unsupported data type: " + json);
        }
      }
      String typeAsText = root.get("type").asText();
      if (typeAsText.startsWith("decimal")) { // Need parsing
        Matcher matcher = decimalPattern.matcher(typeAsText);
        if (matcher.matches()) {
          return new AtomicDataType(
              new DecimalType(
                  nullable,
                  Integer.parseInt(matcher.group(1)),
                  Integer.parseInt(matcher.group(2))));
        } else {
          throw new UnsupportedOperationException("Unsupported data type: " + json);
        }
      }
      switch (typeAsText) {
        case "boolean":
          return new AtomicDataType(new BooleanType(nullable));
        case "binary":
          return new AtomicDataType(new BinaryType());
        case "date":
          return new AtomicDataType(new DateType(nullable));
        case "double":
          return new AtomicDataType(new DoubleType(nullable));
        case "float":
          return new AtomicDataType(new FloatType(nullable));
        case "integer":
          return new AtomicDataType(new IntType(nullable));
        case "long":
          return new AtomicDataType(new BigIntType(nullable));
        case "string":
          return new AtomicDataType(new VarCharType());
        case "timestamp":
          return new AtomicDataType(new LocalZonedTimestampType(nullable, 6));
        case "timestamp_ntz":
          return new AtomicDataType(new TimestampType(nullable, 6));
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + json);
      }
    } catch (Exception e) {
      throw new RuntimeException("Unsupported entry: " + json, e);
    }
  }

  @Override
  public Schema getUnresolvedSchema() {
    return schema;
  }

  @Override
  public boolean isPartitioned() {
    return false;
  }

  @Override
  public List<String> getPartitionKeys() {
    return List.of();
  }

  @Override
  public CatalogTable copy(Map<String, String> options) {
    return new FlinkUnityCatalogTable(
        name, schema, dataFormat, comment, description, options, endpoint, token);
  }

  @Override
  public Map<String, String> getOptions() {
    if ("DELTA".equals(dataFormat)) {
      return Map.of(
          FactoryUtil.CONNECTOR.key(),
          DeltaDynamicTableSinkFactory.IDENTIFIER,
          FlinkUnityCatalogFactory.ENDPOINT.key(),
          endpoint.toString(),
          FlinkUnityCatalogFactory.TOKEN.key(),
          token);
    }
    return Map.of();
  }

  @Override
  public String getComment() {
    return comment;
  }

  @Override
  public CatalogBaseTable copy() {
    return new FlinkUnityCatalogTable(
        name, schema, dataFormat, comment, description, properties, endpoint, token);
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }

  @Override
  public Optional<String> getDetailedDescription() {
    return Optional.ofNullable(description);
  }
}
