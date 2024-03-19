import org.apache.spark.sql.catalyst.analysis.UnresolvedTable

object UnresolvedTableShim {
  def createUnresolvedTable(
      tableNameParts: Seq[String],
      commandName: String,
      relationTypeMismatchHint: Option[String] = None) = {
    UnresolvedTable(tableNameParts, commandName)
  }
}