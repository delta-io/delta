# Draft PR Reply: UC_TABLE_ID_KEY comment (line 53, UpdatePOMetricsHook.scala)

**Comment:** "should this be `UC_TABLE_ID_KEY`?"

**Suggested reply:**

`UC_TABLE_ID_STORAGE_KEY` is intentional and distinct from `UC_TABLE_ID_KEY`:

- **`UC_TABLE_ID_KEY`** (`io.unitycatalog.tableId`) — used in `CatalogTable.properties` and `Metadata.configuration` (Databricks-internal connector)
- **`UC_TABLE_ID_STORAGE_KEY`** (`fs.unitycatalog.table.id`) — used in `CatalogTable.storage.properties` (open-source UCSingleCatalog; Delta surfaces this as `option.fs.unitycatalog.table.id`)

`resolveTableId` checks both namespaces: (1) `ct.properties` for `UC_TABLE_ID_KEY` / `UC_TABLE_ID_KEY_OLD`, and (2) `ct.storage.properties` for `UC_TABLE_ID_STORAGE_KEY`. The storage key is required for open-source UC tables and cannot be replaced with `UC_TABLE_ID_KEY`.
