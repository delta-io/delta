# Workload Based Variant Fields

This protocol change adds a new table property which can be used to identify variant fields for stats collection.

***Update the `Table Properties` section***

> Please add an entry after the `delta.writePartitionColumnsToParquet` table property in the table with the description and details about the new `delta.workloadBasedVariantFields.deltaFileStatistics` table property.

Property | Description | Details
-|-|-
`delta.workloadBasedVariantFields.deltaFileStatistics` | List of variant columns optionally paired with paths indicating the list of variant fields writers COULD prioritize collecting data skipping statistics on if they support this table property and if physical constraints are evaluated to permit these stats. The same variant column name may be provided multiple times if the table is believed to benefit from stats in multiple fields within that column. The property by no means introduces an additional requirement for writers and instead acts as a hint. The purpose of the property is to list variant fields that could potentially benefit from data skipping stats based on filters that were used on the table in the past. | Comma separated list of column names optionally paired with [normalized JSON extraction paths](https://www.rfc-editor.org/rfc/rfc9535.html#name-normalized-paths) without the leading dollar sign ($). Column name-JSON path pairs must be separated by a colon (:). If the column identifier has special characters such as !@#$%^&*()_+-={}\|[]:";'<>,.?/, it must be enclosed in backticks (\`) to escape the special characters. Paths to indicate variant columns nested within structs must be separated by periods (.). When the path being specified is the top-level variant itself ($), the column name alone is sufficient. Example value of table property: ``col_1:['top_level_field'],struct_col.`special,nested_col`:['object_field']['nested_field'],col3``
