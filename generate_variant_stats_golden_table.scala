/*
 * Script to generate a golden table with variant stats in DBR (NO CHECKPOINT).
 *
 * INSTRUCTIONS:
 * 1. Run this script in Databricks Runtime (DBR 15.4+ or in ../runtime with variant stats support)
 * 2. This creates the SAME table structure as spark-variant-checkpoint but WITHOUT a checkpoint
 * 3. After running, copy the generated table to:
 *    spark/src/test/resources/delta/variant-stats-no-checkpoint/
 *
 * To run in DBR:
 * - Open a Databricks notebook in ../runtime
 * - Copy and paste this code
 * - Run the cell
 * - Copy the generated directory to Delta OSS test resources
 */

val tablePath = "/tmp/variant_stats_no_checkpoint_" + System.currentTimeMillis()
println(s"Creating table at: $tablePath")

val query = """
  with jsonStrings as (
    select
      id,
      format_string('{"key": %s}', id) as jsonString
    from
      range(0, 100)
  )
  select
    id,
    parse_json(jsonString) as v,
    array(
      parse_json(jsonString),
      null,
      parse_json(jsonString),
      null,
      parse_json(jsonString)
    ) as array_of_variants,
    named_struct('v', parse_json(jsonString)) as struct_of_variants,
    map(
      cast(id as string),
      parse_json(jsonString),
      'nullKey',
      null
    ) as map_of_variants,
    array(
      named_struct('v', parse_json(jsonString)),
      named_struct('v', null),
      null,
      named_struct(
        'v',
        parse_json(jsonString)
      ),
      null,
      named_struct(
        'v',
        parse_json(jsonString)
      )
    ) as array_of_struct_of_variants,
    named_struct(
      'v',
      array(
        null,
        parse_json(jsonString)
      )
    ) as struct_of_array_of_variants
  from
    jsonStrings
"""

// Create table with NO checkpoint (very high checkpoint interval)
val writeToTableSql = s"""
  create or replace table delta.`$tablePath`
  USING DELTA TBLPROPERTIES (delta.checkpointInterval = 999999)
"""

spark.sql(s"${writeToTableSql}\n${query}")

// DO NOT write additional rows - we want NO checkpoint to be created

println(s"\nTable created at: $tablePath")
println(s"\n✓ Table has 100 rows with variant data and stats")

// Verify no checkpoint exists
val checkpointFiles = dbutils.fs.ls(s"$tablePath/_delta_log").filter(_.name.contains("checkpoint"))
if (checkpointFiles.isEmpty) {
  println("✓ SUCCESS: No checkpoint files exist")
} else {
  println("✗ ERROR: Checkpoint files found - something went wrong!")
  checkpointFiles.foreach(f => println(s"  ${f.name}"))
}

// Print the delta log files
println(s"\nDelta log contents:")
dbutils.fs.ls(s"$tablePath/_delta_log").foreach(f => println(s"  ${f.name}"))

println(s"""
=============================================================================
NEXT STEPS:
=============================================================================
1. Copy the generated table from:
   $tablePath

   To your local Delta OSS checkout at:
   spark/src/test/resources/delta/variant-stats-no-checkpoint/

2. In the runtime directory, run:
   cp -r $tablePath /path/to/delta-oss/spark/src/test/resources/delta/variant-stats-no-checkpoint/

3. Verify the directory structure:
   variant-stats-no-checkpoint/
   ├── _delta_log/
   │   ├── 00000000000000000000.json
   │   └── (no checkpoint files)
   └── part-*.parquet (data files)

4. Run the test in CheckpointsSuite.scala in Delta OSS
=============================================================================
""")
