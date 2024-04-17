# Column Mapping Usage Tracking
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/2682**

This RFC proposes an extension for Column Mapping to track where columns have been dropped or renamed during the history of a table.
This allows using the (logical) name of a column as the physical name of a column, while still ensuring that all physical names are unique.
This helps with the disablement of Column Mapping proposed in [#2481](https://github.com/delta-io/delta/issues/2481), as in this case it is no longer required to rewrite the table, and it simply suffices to change the mode to none.

--------

> New subsection at the end of the `Column Mapping` section

## Usage Tracking

Column Mapping Usage Tracking is an extension of the column mapping feature that allows Delta to track whether a column has been dropped or renamed.
This is tracked by the table property `delta.columnMapping.hasDroppedOrRenamed`. This table property is set to `false` when the table is created, and flipped to `true` when the first column is either dropped or renamed.
The writer table feature `columnMappingUsageTracking` is added to the `writerFeatures` in the `protocol` to ensure that all writers correctly track when columns are dropped or renamed.

--------

> Modification to the `Writer Requirements for Column Mapping` subsection

- Assign a globally unique identifier as the physical name for each new column that is added to the schema. This is especially important for supporting cheap column deletions in `name` mode. In addition, column identifiers need to be assigned to each column. The maximum id that is assigned to a column is tracked as the table property `delta.columnMapping.maxColumnId`. This is an internal table property that cannot be configured by users. This value must increase monotonically as new columns are introduced and committed to the table alongside the introduction of the new columns to the schema.

**is replaced by**

- Assign a unique physical name to each column.
    - When enabling column mapping on existing table, the physical name of the column must be set to the (logical) name of the column.
    - If the feature `columnMappingUsageTracking` is supported, then when adding a new column to a table and `delta.columnMapping.hasDroppedOrRenamed` column property is `false` the (logical) name of the column should be used as the physical name.
    - Otherwise the physical column name must contain a universally unique identifier (UUID) to guarantee uniqueness.
- Assign a column id to each column. The maximum id that is assigned to a column is tracked as the table property `delta.columnMapping.maxColumnId`. This is an internal table property that cannot be configured by users. This value must increase monotonically as new columns are introduced and committed to the table alongside the introduction of the new columns to the schema.

--------

> New subsection at the end of the `Writer Requirements for Column Mapping` subsection

### Writer Requirements for Usage Tracking

In order to support column mapping usage tracking, writers must:
- Write `protocol` and `metaData` actions when Column Mapping Usage Tracking is turned on for the first time:
    - Write a `protocol` action with writer version 7 and the feature `columnMappingUsageTracking` in the `writerFeatures`.
    - Write a `metaData` action with the table property `delta.columnMapping.hasDroppedOrRenamed` set to `false` when creating a new table or enabling the feature on an existing table without column mapping enabled, and set to `true` when enabling usage tracking on an existing table with column mapping enabled.
- When dropping or renaming a column `delta.columnMapping.hasDroppedOrRenamed` must be set to `true`.
- After `delta.columnMapping.hasDroppedOrRenamed` is set to `true` it must never be set back to `false` again.
