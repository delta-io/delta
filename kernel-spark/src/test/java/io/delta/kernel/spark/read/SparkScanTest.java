package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.spark.SparkDsv2TestBase;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanTest extends SparkDsv2TestBase {

  private static String tablePath;
  private static final String tableName = "deltatbl_partition_prune";

  @BeforeAll
  public static void setupGoldenTable(@TempDir File tempDir) {
    createPartitionedTable(tableName, tempDir.getAbsolutePath());
    tablePath = tempDir.getAbsolutePath();
  }

  private final CaseInsensitiveStringMap options =
      new CaseInsensitiveStringMap(
          new java.util.HashMap<String, String>() {
            {
            }
          });
  private final io.delta.kernel.spark.table.SparkTable table =
      new io.delta.kernel.spark.table.SparkTable(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);

  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate cityPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "=",
          new org.apache.spark.sql.connector.expressions.Expression[] {
            FieldReference.apply("city"), LiteralValue.apply("hz", DataTypes.StringType)
          });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate datePredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          "=",
          new org.apache.spark.sql.connector.expressions.Expression[] {
            FieldReference.apply("date"), LiteralValue.apply("20180520", DataTypes.StringType)
          });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate partPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          ">",
          new org.apache.spark.sql.connector.expressions.Expression[] {
            FieldReference.apply("part"), LiteralValue.apply(1, DataTypes.IntegerType)
          });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate dataPredicate =
      new org.apache.spark.sql.connector.expressions.filter.Predicate(
          ">",
          new org.apache.spark.sql.connector.expressions.Expression[] {
            FieldReference.apply("cnt"), LiteralValue.apply(10, DataTypes.IntegerType)
          });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate
      negativeCityPredicate =
          new org.apache.spark.sql.connector.expressions.filter.Predicate(
              "=",
              new org.apache.spark.sql.connector.expressions.Expression[] {
                FieldReference.apply("city"), LiteralValue.apply("zz", DataTypes.StringType)
              });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate
      interColPredicate =
          new org.apache.spark.sql.connector.expressions.filter.Predicate(
              "!=",
              new org.apache.spark.sql.connector.expressions.Expression[] {
                FieldReference.apply("city"), FieldReference.apply("date")
              });
  protected static final org.apache.spark.sql.connector.expressions.filter.Predicate
      negativeInterColPredicate =
          new org.apache.spark.sql.connector.expressions.filter.Predicate(
              "=",
              new org.apache.spark.sql.connector.expressions.Expression[] {
                FieldReference.apply("city"), FieldReference.apply("date")
              });
  // a full set of cities in the golden table, repsents all partitions
  protected static final List<String> allCities =
      Arrays.asList("city=hz", "city=sh", "city=bj", "city=sz");

  @Test
  public void testDPP_singleFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {cityPredicate},
        Arrays.asList("city=hz"));

    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {datePredicate},
        Arrays.asList("date=20180520"));
  }

  @Test
  public void testDPP_multiFilters() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          cityPredicate, datePredicate
        },
        Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ANDFilters() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "AND",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, datePredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {andPredicate},
        Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ORFilters() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, datePredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {orPredicate},
        Arrays.asList("city=hz", "date=20180520"));
  }

  @Test
  public void testDPP_NOTFilter() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate notPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "NOT", new org.apache.spark.sql.connector.expressions.Expression[] {cityPredicate});
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {notPredicate},
        Arrays.asList("city=sh", "city=bj", "city=sz"));
  }

  @Test
  public void testDPP_INFilter() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate inPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "IN",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              FieldReference.apply("city"),
              LiteralValue.apply("hz", DataTypes.StringType),
              LiteralValue.apply("sh", DataTypes.StringType)
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {inPredicate},
        Arrays.asList("city=hz", "city=sh"));
  }

  @Test
  public void testDPP_negativeFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {negativeCityPredicate},
        Arrays.asList());
  }

  @Test
  public void testDPP_ANDNegativeFilter() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "AND",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, negativeCityPredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {andPredicate},
        Arrays.asList());
  }

  @Test
  public void testDPP_ORNegativeFilter() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, negativeCityPredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {orPredicate},
        Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          cityPredicate, dataPredicate
        },
        Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilterOnly() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {dataPredicate},
        allCities);
  }

  @Test
  public void testDPP_ANDDataPredicate() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "AND",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, dataPredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {andPredicate},
        allCities);
  }

  @Test
  public void testDPP_ORDataPredicate() throws Exception {
    org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR",
            new org.apache.spark.sql.connector.expressions.Expression[] {
              cityPredicate, dataPredicate
            });
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {orPredicate},
        allCities);
  }

  @Test
  public void testDPP_interColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {interColPredicate},
        allCities);
  }

  @Test
  public void testDPP_negativeInterColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          negativeInterColPredicate
        },
        Arrays.asList());
  }

  @Test
  public void testDPP_integerFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {partPredicate},
        Arrays.asList("part=2"));
  }

  protected static void checkSupportsRuntimeFilters(
      io.delta.kernel.spark.table.SparkTable table,
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
}
