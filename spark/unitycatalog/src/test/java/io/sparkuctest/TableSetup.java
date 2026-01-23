/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest;

import com.google.common.base.Preconditions;
import io.sparkuctest.UCDeltaTableIntegrationBaseTest.TableType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableSetup {

  private String ddlCommand = "CREATE";
  private String catalogName;
  private String schemaName;
  private String tableName;
  private String tableSchema;
  private List<String> partitionFields;
  private List<String> clusterFields;
  private List<String> tableProperties = new ArrayList<>();
  private String tableComment;
  private TableType tableType;
  private String externalLocation;
  private String asSelect;

  public TableSetup() {}

  public TableSetup setDdlCommand(String ddlCommand) {
    this.ddlCommand = ddlCommand;
    return this;
  }

  public TableSetup setCatalogName(String catalogName) {
    this.catalogName = catalogName;
    return this;
  }

  public TableSetup setSchemaName(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  public TableSetup setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TableSetup setTableSchema(String tableSchema) {
    this.tableSchema = tableSchema;
    return this;
  }

  public TableSetup setPartitionFields(String... partitionFields) {
    this.partitionFields = Arrays.asList(partitionFields);
    return this;
  }

  public TableSetup setClusterFields(String... clusterFields) {
    this.clusterFields = Arrays.asList(clusterFields);
    return this;
  }

  public TableSetup setTableProperties(String... tableProperties) {
    this.tableProperties = Arrays.asList(tableProperties);
    return this;
  }

  public TableSetup setTableComment(String tableComment) {
    this.tableComment = tableComment;
    return this;
  }

  public TableSetup setTableType(TableType tableType) {
    this.tableType = tableType;
    return this;
  }

  public TableSetup setExternalLocation(String externalLocation) {
    this.externalLocation = externalLocation;
    return this;
  }

  public TableSetup setAsSelect(String asSelect) {
    this.asSelect = asSelect;
    return this;
  }

  public String fullTableName() {
    return String.format("%s.%s.%s", catalogName, schemaName, tableName);
  }

  public String toSql() {
    StringBuilder sb = new StringBuilder();
    sb.append(ddlCommand);
    sb.append(" TABLE ");
    sb.append(fullTableName());

    // Add the schema clause.
    if (tableSchema != null && !tableSchema.isEmpty()) {
      Preconditions.checkArgument(
          asSelect == null || asSelect.isEmpty(),
          "Cannot specify table schema when use AS SELECT.");
      sb.append("( ").append(tableSchema).append(" )");
    }

    // Add the USING DELTA clause.
    sb.append(" USING DELTA ");

    // Add the PARTITIONED BY clause.
    if (partitionFields != null && !partitionFields.isEmpty()) {
      Preconditions.checkArgument(
          clusterFields == null || clusterFields.isEmpty(),
          "Cannot set both partition fields and cluster fields");
      sb.append(" PARTITIONED BY ")
          .append("(")
          .append(String.join(",", partitionFields))
          .append(")");
    }

    // Add the CLUSTER BY clause.
    if (clusterFields != null && !clusterFields.isEmpty()) {
      Preconditions.checkArgument(
          partitionFields == null || partitionFields.isEmpty(),
          "Cannot set both partition fields and cluster fields");
      sb.append(" CLUSTER BY ").append("(").append(String.join(",", clusterFields)).append(")");
    }

    // Add the TBLPROPERTIES clause.
    if (tableType == TableType.MANAGED) {
      // Always add the 'delta.feature.catalogManaged' for managed table.
      if (tableProperties.stream()
          .noneMatch(prop -> prop.contains("delta.feature.catalogManaged"))) {
        tableProperties.add("'delta.feature.catalogManaged'='supported'");
      }
    }
    if (!tableProperties.isEmpty()) {
      sb.append(" TBLPROPERTIES ")
          .append("(")
          .append(String.join(",", tableProperties))
          .append(")");
    }

    // Add the comment clause.
    if (tableComment != null && !tableComment.isEmpty()) {
      sb.append(" COMMENT ").append("'").append(tableComment).append("'");
    }

    // Add the LOCATION clause if external table.
    if (tableType == TableType.EXTERNAL) {
      Preconditions.checkArgument(
          externalLocation != null && !externalLocation.isEmpty(),
          "External location is required for external table.");
      sb.append(" LOCATION ").append("'").append(externalLocation).append("'");
    }

    // Add the AS SELECT clause.
    if (asSelect != null && !asSelect.isEmpty()) {
      Preconditions.checkArgument(
          tableSchema == null || tableSchema.isEmpty(),
          "Cannot specify table schema when use AS SELECT.");
      sb.append(" AS SELECT ").append(asSelect);
    }

    return sb.toString();
  }
}
