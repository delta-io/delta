# Slowly Changing Dimension (SCD) Type 2 Implementation with Delta Lake

This example demonstrates how to implement a Slowly Changing Dimension (SCD) Type 2 pattern using Delta Lake. SCD Type 2 is a dimensional modeling technique that preserves the historical values of dimension attributes by creating new records when changes occur.

## Features Demonstrated

- Creation of a Delta table with SCD Type 2 tracking columns
- Implementation of merge operations to handle updates
- Maintenance of historical records while preserving data lineage
- Efficient querying of current and historical data

## Key Delta Lake Features Used

- MERGE operations for atomic updates
- Time travel capabilities
- Schema enforcement
- Transaction support

## Prerequisites

- Apache Spark 3.x
- Delta Lake 2.x
- Scala 2.12 or later

## Running the Example

1. Ensure you have the necessary dependencies in your `build.sbt`:

```scala
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
```

2. Run the example:

```bash
spark-submit --packages io.delta:delta-core_2.12:2.4.0 SCDType2Example.scala
```

## Output Explanation

The example will show:
1. Initial state of the customer dimension
2. Updated state after changes (new email and address for John Doe)
3. Historical record preservation
4. Queries demonstrating how to access current and historical data

## Best Practices Demonstrated

- Proper handling of temporal attributes (valid_from, valid_to)
- Atomic updates using merge operations
- Efficient filtering of current records
- Maintenance of data lineage