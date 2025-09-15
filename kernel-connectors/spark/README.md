# Delta Kernel – Spark Connector

The **Delta Kernel Spark Connector** enables Apache Spark to read Delta tables using the [Delta Kernel](https://github.com/delta-io/delta).  
It leverages Spark’s DataSource V2 (DSV2) APIs to integrate Delta Kernel into Spark’s query execution pipeline.

---

## High-Level Design

- **Delta Kernel**
  - Resolves table state from Catalog and Delta logs.
  - Performs file skipping to minimize unnecessary reads.

- **Spark Engine (via DSV2)**
  - Pushes down static and dynamic predicates.
  - Partitions files (default 128MB per split) for efficient scheduling.
  - Executes Parquet file scans with Spark’s existing `ParquetFileFormat`.

<p align="center">
  <img width="600" alt="Delta-kernel-connector" src="https://github.com/user-attachments/assets/50d65443-771f-4f03-9d19-7e5fb22977f7" />
</p>

