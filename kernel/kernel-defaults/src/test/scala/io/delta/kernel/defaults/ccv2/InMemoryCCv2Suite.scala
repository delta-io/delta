package io.delta.kernel.defaults.ccv2

import io.delta.kernel.Operation
import io.delta.kernel.ccv2.ResolvedTable
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.ccv2.setup.{CCv2Client, InMemoryCatalogClient}
import io.delta.kernel.utils.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class InMemoryCCv2Suite extends AnyFunSuite with DeltaTableWriteSuiteBase {

  private val catalogClient = new InMemoryCatalogClient()
  private val ccv2Client = new CCv2Client(defaultEngine, catalogClient)


  test("aaa catalog client > staging table") {
    val stagingTableMetadata = ccv2Client.getStagingTableResolvedMetadata(
      "table_000", defaultEngine, catalogClient)
    println(stagingTableMetadata.getPath)
    println(stagingTableMetadata.getVersion)

    val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, stagingTableMetadata)

    val txn = resolvedTable
      .createTransactionBuilder(testEngineInfo, Operation.CREATE_TABLE)
      .withSchema(defaultEngine, testSchema)
      .build(defaultEngine)

    val txnState = txn.getTransactionState(defaultEngine)
    val stagedFiles = stageData(txnState, Map.empty, dataBatches1)
    val stagedActionsIterable = CloseableIterable.inMemoryIterable(stagedFiles)
    txn.commit(defaultEngine, stagedActionsIterable)
  }

}
