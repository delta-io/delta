package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.spark.internal.v2.SparkDsv2TestBase;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanTest extends SparkDsv2TestBase {

  private static String tablePath;
  private static final String tableName = "deltatbl_partitioned";

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    createPartitionedTable(tableName, tempDir.getAbsolutePath());
    tablePath = tempDir.getAbsolutePath();
  }

  private final CaseInsensitiveStringMap options =
      new CaseInsensitiveStringMap(new java.util.HashMap<>());

  private final SparkTable table =
      new SparkTable(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);

  protected static final Predicate cityPredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("city"), LiteralValue.apply("hz", DataTypes.StringType)
          });

  protected static final Predicate datePredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("date"), LiteralValue.apply("20180520", DataTypes.StringType)
          });

  protected static final Predicate partPredicate =
      new Predicate(
          ">",
          new Expression[] {
            FieldReference.apply("part"), LiteralValue.apply(1, DataTypes.IntegerType)
          });

  protected static final Predicate dataPredicate =
      new Predicate(
          ">",
          new Expression[] {
            FieldReference.apply("cnt"), LiteralValue.apply(10, DataTypes.IntegerType)
          });

  protected static final Predicate negativeCityPredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("city"), LiteralValue.apply("zz", DataTypes.StringType)
          });

  protected static final Predicate interColPredicate =
      new Predicate(
          "!=", new Expression[] {FieldReference.apply("city"), FieldReference.apply("date")});

  protected static final Predicate negativeInterColPredicate =
      new Predicate(
          "=", new Expression[] {FieldReference.apply("city"), FieldReference.apply("date")});

  // a full set of cities in the golden table, repsents all partitions
  protected static final List<String> allCities =
      Arrays.asList("city=hz", "city=sh", "city=bj", "city=sz");

  @Test
  public void testDPP_singleFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {cityPredicate}, Arrays.asList("city=hz"));

    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {datePredicate}, Arrays.asList("date=20180520"));

    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {partPredicate}, Arrays.asList("part=2"));
  }

  @Test
  public void testDPP_multiFilters() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new Predicate[] {cityPredicate, datePredicate},
        Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ANDFilters() throws Exception {
    Predicate andPredicate = new Predicate("AND", new Expression[] {cityPredicate, datePredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {andPredicate}, Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ORFilters() throws Exception {
    Predicate orPredicate = new Predicate("OR", new Expression[] {cityPredicate, datePredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {orPredicate}, Arrays.asList("city=hz", "date=20180520"));
  }

  @Test
  public void testDPP_NOTFilter() throws Exception {
    Predicate notPredicate = new Predicate("NOT", new Expression[] {cityPredicate});
    checkSupportsRuntimeFilters(
        table,
        options,
        new Predicate[] {notPredicate},
        Arrays.asList("city=sh", "city=bj", "city=sz"));
  }

  @Test
  public void testDPP_INFilter() throws Exception {
    Predicate inPredicate =
        new Predicate(
            "IN",
            new Expression[] {
              FieldReference.apply("city"),
              LiteralValue.apply("hz", DataTypes.StringType),
              LiteralValue.apply("sh", DataTypes.StringType)
            });
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {inPredicate}, Arrays.asList("city=hz", "city=sh"));
  }

  @Test
  public void testDPP_negativeFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {negativeCityPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_ANDNegativeFilter() throws Exception {
    Predicate andPredicate =
        new Predicate("AND", new Expression[] {cityPredicate, negativeCityPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {andPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_ORNegativeFilter() throws Exception {
    Predicate orPredicate =
        new Predicate("OR", new Expression[] {cityPredicate, negativeCityPredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {orPredicate}, Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {cityPredicate, dataPredicate}, Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilterOnly() throws Exception {
    checkSupportsRuntimeFilters(table, options, new Predicate[] {dataPredicate}, allCities);
  }

  @Test
  public void testDPP_ANDDataPredicate() throws Exception {
    Predicate andPredicate = new Predicate("AND", new Expression[] {cityPredicate, dataPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {andPredicate}, allCities);
  }

  @Test
  public void testDPP_ORDataPredicate() throws Exception {
    Predicate orPredicate = new Predicate("OR", new Expression[] {cityPredicate, dataPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {orPredicate}, allCities);
  }

  @Test
  public void testDPP_interColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(table, options, new Predicate[] {interColPredicate}, allCities);
  }

  @Test
  public void testDPP_negativeInterColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {negativeInterColPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_integerFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {partPredicate}, Arrays.asList("part=2"));
  }

  protected static void checkSupportsRuntimeFilters(
      SparkTable table,
      CaseInsensitiveStringMap scanOptions,
      org.apache.spark.sql.connector.expressions.filter.Predicate[] runtimeFilters,
      List<String> remainingPartitionValueAfterDpp)
      throws Exception {
    ScanBuilder newBuilder = table.newScanBuilder(scanOptions);
    SparkScanBuilder builder = (SparkScanBuilder) newBuilder;
    Scan scan = builder.build();
    SparkScan sparkScan = (SparkScan) scan;

    List<PartitionedFile> beforeDppFiles = getPartitionedFiles(sparkScan);
    // make a copy for comparison after DPP
    beforeDppFiles = new ArrayList<>(beforeDppFiles);
    long beforeDppTotalBytes = getTotalBytes(sparkScan);
    assert (beforeDppFiles.size() == 5);

    sparkScan.filter(runtimeFilters);
    List<PartitionedFile> afterDppFiles = getPartitionedFiles(sparkScan);
    long afterDppTotalBytes = getTotalBytes(sparkScan);
    assert (beforeDppFiles.containsAll(afterDppFiles));
    assert (beforeDppTotalBytes >= afterDppTotalBytes);

    List<PartitionedFile> expectedPartitionFilesAfterDpp = new ArrayList<>();
    long expectedTotalBytesAfterDpp = 0;
    for (PartitionedFile pf : beforeDppFiles) {
      for (String partitionValue : remainingPartitionValueAfterDpp) {
        if (pf.filePath().toString().contains(partitionValue)) {
          expectedPartitionFilesAfterDpp.add(pf);
          expectedTotalBytesAfterDpp += pf.fileSize();
          break;
        }
      }
    }

    assertEquals(expectedPartitionFilesAfterDpp.size(), afterDppFiles.size());
    assertEquals(new HashSet<>(expectedPartitionFilesAfterDpp), new HashSet<>(afterDppFiles));
    assertEquals(expectedTotalBytesAfterDpp, afterDppTotalBytes);
  }

  private static List<PartitionedFile> getPartitionedFiles(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("partitionedFiles");
    field.setAccessible(true);
    return (List<PartitionedFile>) field.get(scan);
  }

  private static long getTotalBytes(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("totalBytes");
    field.setAccessible(true);
    return (long) field.get(scan);
  }

  // ================================================================================================
  // Tests for streaming options validation
  // ================================================================================================

  @Test
  public void testValidateStreamingOptions_SupportedOptions() {
    // Test with supported options (case insensitive) and custom user options
    Map<String, String> javaOptions = new HashMap<>();
    javaOptions.put("startingVersion", "0");
    javaOptions.put("MaxFilesPerTrigger", "100");
    javaOptions.put("MAXBYTESPERTRIGGER", "1g");
    javaOptions.put("myCustomOption", "value");
    scala.collection.immutable.Map<String, String> supportedOptions =
        ScalaUtils.toScalaMap(javaOptions);
    DeltaOptions deltaOptions = new DeltaOptions(supportedOptions, spark.sessionState().conf());

    // Verify DeltaOptions can recognize the options (case insensitive)
    assertEquals(true, deltaOptions.maxFilesPerTrigger().isDefined());
    assertEquals(100, deltaOptions.maxFilesPerTrigger().get());
    assertEquals(true, deltaOptions.maxBytesPerTrigger().isDefined());

    // Should not throw - supported and custom options are allowed
    SparkScan.validateStreamingOptions(deltaOptions);
  }

  @Test
  public void testValidateStreamingOptions_UnsupportedOptions() {
    // Test with blocked DeltaOptions, supported options, and custom user options
    Map<String, String> javaOptions = new HashMap<>();
    javaOptions.put("startingVersion", "0");
    javaOptions.put("readChangeFeed", "true");
    javaOptions.put("myCustomOption", "value");
    scala.collection.immutable.Map<String, String> mixedOptions =
        ScalaUtils.toScalaMap(javaOptions);
    DeltaOptions deltaOptions = new DeltaOptions(mixedOptions, spark.sessionState().conf());

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SparkScan.validateStreamingOptions(deltaOptions));

    // Verify exact error message - only the blocked option should appear
    // Note: DeltaOptions uses CaseInsensitiveMap which lowercases keys during iteration
    assertEquals(
        "The following streaming options are not supported: [readchangefeed]. "
            + "Supported options are: [startingVersion, maxFilesPerTrigger, maxBytesPerTrigger].",
        exception.getMessage());
  }
}
