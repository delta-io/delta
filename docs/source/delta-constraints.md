---
description: Learn how Delta tables apply constraints.
---

# Constraints

Delta tables support standard SQL constraint management clauses that ensure that the quality and integrity of data added to a table is automatically verified. When a constraint is violated, <Delta> throws an `InvariantViolationException` to signal that the new data can't be added.

.. important::
   Adding a constraint automatically upgrades the table writer protocol version. See [_](/versioning.md) to understand table protocol versioning and what it means to upgrade the protocol version.

Two types of constraints are supported:

- `NOT NULL`: indicates that values in specific columns cannot be null.
- `CHECK`: indicates that a specified Boolean expression must be true for each input row.

## `NOT NULL` constraint

You specify `NOT NULL` constraints in the schema when you create a table and drop `NOT NULL` constraints using the `ALTER TABLE CHANGE COLUMN` command.

```sql
> CREATE TABLE default.people10m (
    id INT NOT NULL,
    firstName STRING,
    middleName STRING NOT NULL,
    lastName STRING,
    gender STRING,
    birthDate TIMESTAMP,
    ssn STRING,
    salary INT
  ) USING DELTA;

> ALTER TABLE default.people10m CHANGE COLUMN middleName DROP NOT NULL;
```

If you specify a `NOT NULL` constraint on a column nested within a struct, the parent struct is also constrained to not be null. However, columns nested within array or map types do not accept `NOT NULL` constraints.

## `CHECK` constraint

You manage `CHECK` constraints using the `ALTER TABLE ADD CONSTRAINT` and `ALTER TABLE DROP CONSTRAINT` commands. `ALTER TABLE ADD CONSTRAINT` verifies that all existing rows satisfy the constraint before adding it to the table.

```sql
> CREATE TABLE default.people10m (
   id INT,
   firstName STRING,
   middleName STRING,
   lastName STRING,
   gender STRING,
   birthDate TIMESTAMP,
   ssn STRING,
   salary INT
 ) USING DELTA;

> ALTER TABLE default.people10m ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');
> ALTER TABLE default.people10m DROP CONSTRAINT dateWithinRange;
```

`CHECK` constraints are table properties in the output of the `DESCRIBE DETAIL` and `SHOW TBLPROPERTIES` commands.

```sql
> ALTER TABLE default.people10m ADD CONSTRAINT validIds CHECK (id > 1 and id < 99999999);

> DESCRIBE DETAIL default.people10m;

> SHOW TBLPROPERTIES default.people10m;
```

.. <Delta> replace:: Delta Lake
.. <AS> replace:: Apache Spark
