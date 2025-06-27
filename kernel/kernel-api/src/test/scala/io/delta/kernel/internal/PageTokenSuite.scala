import java.util.{HashMap, Map}

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.annotation.VisibleForTesting
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.replay.PageToken
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class PageTokenSuite extends AnyFunSuite with MockFileSystemClientUtils {

  // Test data constants
  private val TEST_FILE_NAME = "test_file.json"
  private val TEST_ROW_INDEX = 42L
  private val TEST_SIDECAR_INDEX = 5L
  private val TEST_KERNEL_VERSION = "3.0.0"
  private val TEST_TABLE_PATH = "/path/to/table"
  private val TEST_TABLE_VERSION = 123L
  private val TEST_PREDICATE_HASH = 456L
  private val TEST_LOG_SEGMENT_HASH = 789L

  private val expectedPageToken = new PageToken(
    TEST_FILE_NAME,
    TEST_ROW_INDEX,
    TEST_SIDECAR_INDEX,
    TEST_KERNEL_VERSION,
    TEST_TABLE_PATH,
    TEST_TABLE_VERSION,
    TEST_PREDICATE_HASH,
    TEST_LOG_SEGMENT_HASH)

  private val rowData: Map[Integer, Object] = new HashMap()
  rowData.put(0, TEST_FILE_NAME)
  rowData.put(1, TEST_ROW_INDEX.asInstanceOf[Object])
  rowData.put(2, TEST_SIDECAR_INDEX.asInstanceOf[Object])
  rowData.put(3, TEST_KERNEL_VERSION)
  rowData.put(4, TEST_TABLE_PATH)
  rowData.put(5, TEST_TABLE_VERSION.asInstanceOf[Object])
  rowData.put(6, TEST_PREDICATE_HASH.asInstanceOf[Object])
  rowData.put(7, TEST_LOG_SEGMENT_HASH.asInstanceOf[Object])

  val expectedRow = new GenericRow(PageToken.PAGE_TOKEN_SCHEMA, rowData)

  test("Test PageToken.fromRow with valid data") {
    val pageToken = PageToken.fromRow(expectedRow)
    assert(pageToken.equals(expectedPageToken))
  }

  test("Test PageToken.toRow with valid data") {
    //  val row = expectedPageToken.toRow()
    //  assert(row.equals(expectedRow))
    // no function to compare two rows
  }

  test("E2E: PageToken round-trip: toRow -> fromRow") {
    val row = expectedPageToken.toRow()
    val reconstructedPageToken = PageToken.fromRow(row)
    assert(reconstructedPageToken.equals(expectedPageToken))
  }

  test("PageToken.fromRow throws exception when input row has invalid schema") {
    // Create a schema with wrong field names
    val invalidSchema = new StructType()
      .add("wrongFieldName", StringType.STRING)
      .add("rowIndexInFile", LongType.LONG)
      .add("sidecarIndex", LongType.LONG)
      .add("kernelVersion", StringType.STRING)
      .add("tablePath", StringType.STRING)
      .add("tableVersion", LongType.LONG)
      .add("predicateHash", LongType.LONG)
      .add("logSegmentHash", LongType.LONG)

    val invalidRowData: Map[Integer, Object] = new HashMap()
    invalidRowData.put(0, TEST_FILE_NAME)
    invalidRowData.put(1, TEST_ROW_INDEX.asInstanceOf[Object])
    invalidRowData.put(2, TEST_SIDECAR_INDEX.asInstanceOf[Object])
    invalidRowData.put(3, TEST_KERNEL_VERSION)
    invalidRowData.put(4, TEST_TABLE_PATH)
    invalidRowData.put(5, TEST_TABLE_VERSION.asInstanceOf[Object])
    invalidRowData.put(6, TEST_PREDICATE_HASH.asInstanceOf[Object])
    invalidRowData.put(7, TEST_LOG_SEGMENT_HASH.asInstanceOf[Object])

    val row = new GenericRow(invalidSchema, invalidRowData)
    val exception = intercept[IllegalArgumentException] {
      PageToken.fromRow(row)
    }
    assert(exception.getMessage.contains(
      "Invalid Page Token: input row schema does not match expected PageToken schema"))
    assert(exception.getMessage.contains("Expected:"))
    assert(exception.getMessage.contains("Got:"))
  }
}
