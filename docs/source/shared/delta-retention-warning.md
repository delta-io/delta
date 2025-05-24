.. warning::

  It is recommended that you set a retention interval to be at least 7 days,
  because old snapshots and uncommitted files can still be in use by concurrent
  readers or writers to the table.  If `VACUUM` cleans up active files,
  concurrent readers can fail or, worse, tables can be corrupted when `VACUUM`
  deletes files that have not yet been committed. You must choose an interval
  that is longer than the longest running concurrent transaction and the longest
  period that any stream can lag behind the most recent update to the table.
