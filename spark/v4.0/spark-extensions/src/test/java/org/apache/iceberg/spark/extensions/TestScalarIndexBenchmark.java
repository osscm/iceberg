/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark the SCALAR index: measures data read and query wall-clock for equality, {@code IN}, and
 * range queries WITH vs WITHOUT the index on a synthetic table (default 10,000,000 rows), via the
 * embedded Spark session. A single IDENTITY index serves all three query shapes.
 *
 * <p>Not a correctness test in the usual sense: it does assert that the index never changes the
 * result set, but its purpose is the printed before/after comparison. Because {@code
 * Dataset#inputFiles()} does not reflect Iceberg's DataSourceV2 scans in Spark 3.5, the observable
 * signals here are Spark task input metrics (bytes/records read, which drop when {@code
 * FileScanTaskFilteringScan} returns fewer tasks) and wall-clock.
 *
 * <p>Disabled unless the {@code SCALAR_BENCHMARK} environment variable is {@code true}: it writes
 * ten million rows per scenario and catalog, which is far too slow for the normal suite. Run it
 * explicitly:
 *
 * <pre>
 *   SCALAR_BENCHMARK=true ./gradlew :iceberg-spark:iceberg-spark-extensions-4.0_2.13:test --rerun \
 *     --tests "org.apache.iceberg.spark.extensions.TestScalarIndexBenchmark"
 * </pre>
 *
 * Results are logged and also written to {@code
 * build/scalar-index-benchmark-&lt;catalog&gt;-&lt;scenario&gt;.txt}.
 */
@EnabledIfEnvironmentVariable(named = "SCALAR_BENCHMARK", matches = "true")
@ExtendWith(ParameterizedTestExtension.class)
public class TestScalarIndexBenchmark extends ExtensionsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestScalarIndexBenchmark.class);

  private static final long NUM_ROWS = 10_000_000L;
  private static final int NUM_FILES = 200;
  private static final int RUNS = 3;
  // Number of consecutive ids spanned by the BETWEEN (range) query.
  private static final int RANGE_WIDTH = 20;
  // Low-cardinality partition count for the partitioned scenario (part = id % NUM_PARTITIONS).
  private static final int NUM_PARTITIONS = 10;
  // Rows in the file appended after the index build in the uncovered-file scenario.
  private static final long UNCOVERED_ROWS = 2L;
  private static final String LISTING_CACHE_TTL_CONF =
      "spark.sql.iceberg.scalar-index.listing-cache-ttl-ms";

  /** Point/IN/range query shapes on an unpartitioned table (all served by one IDENTITY index). */
  private List<String[]> unpartitionedCases() {
    long quarter = NUM_ROWS / 4;
    long half = NUM_ROWS / 2;
    long threeQuarter = 3 * NUM_ROWS / 4;
    return List.of(
        new String[] {
          "eq (id=" + half + ")", String.format("SELECT * FROM %s WHERE id = %d", tableName, half)
        },
        new String[] {
          "in (3 ids)",
          String.format(
              "SELECT * FROM %s WHERE id IN (%d, %d, %d)", tableName, quarter, half, threeQuarter)
        },
        new String[] {
          "range (" + RANGE_WIDTH + " ids)",
          String.format(
              "SELECT * FROM %s WHERE id BETWEEN %d AND %d",
              tableName, half, half + RANGE_WIDTH - 1)
        });
  }

  /**
   * The realistic combo: table partitioned by a low-cardinality column, SCALAR index on a
   * high-cardinality non-partition column. Each query also constrains the partition, so the
   * baseline already benefits from partition pruning; the index then prunes files *within* the
   * surviving partition. All chosen ids are multiples of NUM_PARTITIONS, so they land in part 0.
   */
  private List<String[]> partitionedCases() {
    long quarter = NUM_ROWS / 4;
    long half = NUM_ROWS / 2;
    long threeQuarter = 3 * NUM_ROWS / 4;
    return List.of(
        new String[] {
          "eq (part=0,id=" + half + ")",
          String.format("SELECT * FROM %s WHERE part = 0 AND id = %d", tableName, half)
        },
        new String[] {
          "in (part=0,3 ids)",
          String.format(
              "SELECT * FROM %s WHERE part = 0 AND id IN (%d, %d, %d)",
              tableName, quarter, half, threeQuarter)
        },
        new String[] {
          "range (part=0," + RANGE_WIDTH + " ids)",
          String.format(
              "SELECT * FROM %s WHERE part = 0 AND id BETWEEN %d AND %d",
              tableName, half, half + (long) RANGE_WIDTH * NUM_PARTITIONS - 1)
        });
  }

  @AfterEach
  public void removeTable() {
    spark.conf().unset(LISTING_CACHE_TTL_CONF);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void benchmarkUnpartitioned() throws Exception {
    createUnpartitionedTable();
    runBenchmark("unpartitioned", "files=" + NUM_FILES, "'id'", "IDENTITY", unpartitionedCases());
  }

  /** Same as {@link #benchmarkUnpartitioned()} with the index listing cache disabled. */
  @TestTemplate
  public void benchmarkUnpartitionedListingCacheOff() throws Exception {
    spark.conf().set(LISTING_CACHE_TTL_CONF, "0");
    createUnpartitionedTable();
    runBenchmark(
        "unpartitioned-listing-cache-off",
        "files=" + NUM_FILES,
        "'id'",
        "IDENTITY",
        unpartitionedCases());
  }

  private void createUnpartitionedTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // Round-robin shuffle into many files so each file's id min/max spans ~the whole range;
    // otherwise per-file stats would prune the baseline too, hiding the index's benefit.
    spark
        .range(0, NUM_ROWS)
        .withColumn("data", functions.concat(functions.lit("row-"), functions.col("id")))
        .repartition(NUM_FILES)
        .writeTo(tableName)
        .append();
  }

  /**
   * Keys that are absent but inside every file's min/max, so column stats cannot prune them and
   * only the index can. Ids are even, so every odd id in range is absent.
   */
  private List<String[]> absentKeyCases() {
    long half = NUM_ROWS;
    return List.of(
        new String[] {
          "absent eq (id=" + (half + 1) + ")",
          String.format("SELECT * FROM %s WHERE id = %d", tableName, half + 1)
        },
        new String[] {
          "absent in (3 ids)",
          String.format(
              "SELECT * FROM %s WHERE id IN (%d, %d, %d)",
              tableName, half / 2 + 1, half + 1, 3 * half / 2 + 1)
        },
        new String[] {
          "present eq (id=" + half + ")",
          String.format("SELECT * FROM %s WHERE id = %d", tableName, half)
        });
  }

  private void createEvenIdTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    spark
        .range(0, NUM_ROWS)
        .withColumn("id", functions.col("id").multiply(2))
        .withColumn("data", functions.concat(functions.lit("row-"), functions.col("id")))
        .repartition(NUM_FILES)
        .writeTo(tableName)
        .append();
  }

  @TestTemplate
  public void benchmarkAbsentKey() throws Exception {
    createEvenIdTable();
    List<Measurement> indexed =
        runBenchmark("absent-key", "files=" + NUM_FILES, "'id'", "IDENTITY", absentKeyCases());

    // Index pruning must plan no files for absent keys, and still find the present one.
    assertThat(indexed.get(0).recordsRead).isZero();
    assertThat(indexed.get(1).recordsRead).isZero();
    assertThat(indexed.get(2).recordsRead).isPositive();
  }

  /**
   * Absent keys with one data file appended after the index was built. That file has bounds
   * spanning the whole id range, so it cannot be skipped by stats or the index and is the only file
   * read.
   */
  @TestTemplate
  public void benchmarkAbsentKeyWithUncoveredFile() throws Exception {
    createEvenIdTable();
    List<Measurement> indexed =
        runBenchmark(
            "absent-key-uncovered",
            "files=" + NUM_FILES + "+1 uncovered",
            "'id'",
            "IDENTITY",
            absentKeyCases(),
            this::appendUncoveredFile);

    assertThat(indexed.get(0).recordsRead).isEqualTo(UNCOVERED_ROWS);
    assertThat(indexed.get(1).recordsRead).isEqualTo(UNCOVERED_ROWS);
  }

  private void appendUncoveredFile() {
    // Even ids below and above the table's range: no query matches them, but the file's bounds do
    // cover every queried id. coalesce keeps the rows in a single file.
    spark
        .sql(
            String.format(
                "SELECT /*+ COALESCE(1) */ * FROM VALUES"
                    + " (-2L, 'low'), (%dL, 'high') AS t(id, data)",
                4 * NUM_ROWS))
        .createOrReplaceTempView("uncovered_rows");
    sql("INSERT INTO TABLE %s SELECT * FROM uncovered_rows", tableName);
  }

  @TestTemplate
  public void benchmarkPartitioned() throws Exception {
    // distribution-mode=none + fanout keeps our round-robin layout instead of re-clustering each
    // partition into a single file -- we need multiple files per partition for the index to prune
    // between inside a partition.
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string, part int NOT NULL) USING iceberg"
            + " PARTITIONED BY (part)"
            + " TBLPROPERTIES ('write.distribution-mode'='none',"
            + " 'write.spark.fanout.enabled'='true')",
        tableName);
    // part = id % NUM_PARTITIONS spreads rows across partitions. Round-robin into
    // NUM_FILES/NUM_PARTITIONS tasks then yields ~that many files per partition, with ids scattered
    // within each file, so per-file stats cannot prune inside a partition -- only partition pruning
    // and the index can.
    spark
        .range(0, NUM_ROWS)
        .withColumn("part", functions.col("id").mod(NUM_PARTITIONS).cast("int"))
        .withColumn("data", functions.concat(functions.lit("row-"), functions.col("id")))
        .repartition(NUM_FILES / NUM_PARTITIONS)
        .writeTo(tableName)
        .append();
    runBenchmark(
        "partitioned",
        "partitions=" + NUM_PARTITIONS + " files/partition=" + (NUM_FILES / NUM_PARTITIONS),
        "'id'",
        "IDENTITY",
        partitionedCases());
  }

  @TestTemplate
  public void benchmarkComposite() throws Exception {
    // Composite ZORDER index on a (bigint, varchar) key -- the realistic mixed-type case. k1 is
    // unique and k2 = "v" + k1; queries constrain both. Round-robin scatter so per-file stats
    // cannot prune the baseline.
    sql("CREATE TABLE %s (k1 bigint NOT NULL, k2 string, data string) USING iceberg", tableName);
    spark
        .range(0, NUM_ROWS)
        .withColumnRenamed("id", "k1")
        .withColumn("k2", functions.concat(functions.lit("v"), functions.col("k1")))
        .withColumn("data", functions.concat(functions.lit("row-"), functions.col("k1")))
        .repartition(NUM_FILES)
        .writeTo(tableName)
        .append();
    runBenchmark("composite", "files=" + NUM_FILES, "'k1', 'k2'", "ZORDER", compositeCases());
  }

  /** Composite equality queries constraining both key columns, for interior keys. */
  private List<String[]> compositeCases() {
    return List.of(
        compositeCase(NUM_ROWS / 4),
        compositeCase(NUM_ROWS / 2),
        compositeCase(3 * NUM_ROWS / 4));
  }

  private String[] compositeCase(long key) {
    return new String[] {
      "and (k1=" + key + ")",
      String.format("SELECT * FROM %s WHERE k1 = %d AND k2 = 'v%d'", tableName, key, key)
    };
  }

  /**
   * Shared driver: measure each case on the already-populated table, build the IDENTITY index, then
   * re-measure (asserting identical results), and write the before/after report.
   */
  private List<Measurement> runBenchmark(
      String scenario, String layout, String columnsArray, String transform, List<String[]> cases)
      throws Exception {
    return runBenchmark(scenario, layout, columnsArray, transform, cases, () -> {});
  }

  /**
   * Same as above, running {@code afterIndexBuild} between the index build and the indexed
   * measurements, e.g. to append files the index does not cover. Returns the indexed measurements.
   */
  private List<Measurement> runBenchmark(
      String scenario,
      String layout,
      String columnsArray,
      String transform,
      List<String[]> cases,
      Runnable afterIndexBuild)
      throws Exception {
    InputMetricsListener listener = new InputMetricsListener();
    spark.sparkContext().addSparkListener(listener);

    StringBuilder report = new StringBuilder();
    report.append(String.format("SCALAR index query benchmark (%s)%n", scenario));
    report.append(
        String.format(
            "catalog=%s rows=%d %s transform=%s%n", catalogName, NUM_ROWS, layout, transform));

    List<Measurement> indexed = Lists.newArrayList();
    try {
      List<Measurement> baseline = Lists.newArrayList();
      for (String[] queryCase : cases) {
        baseline.add(measure(queryCase[1], listener));
      }

      long buildStart = System.nanoTime();
      sql(
          "CALL %s.system.build_scalar_index(table => '%s', columns => array(%s),"
              + " transform => '%s')",
          catalogName, tableIdent, columnsArray, transform);
      long buildMs = (System.nanoTime() - buildStart) / 1_000_000L;
      afterIndexBuild.run();

      for (int i = 0; i < cases.size(); i++) {
        // Measured before any other indexed query so its first run reflects cold caches.
        Measurement after = measure(cases.get(i)[1], listener);
        // The index must only change speed, never results.
        assertThat(after.rows).containsExactlyInAnyOrderElementsOf(baseline.get(i).rows);
        indexed.add(after);
      }

      report.append(String.format("index build: %d ms (full COW rebuild)%n%n", buildMs));
      report.append(
          String.format(
              "%-26s %-24s %-26s %-18s %-18s%n",
              "query",
              "records read b/a",
              "bytes read b/a",
              "first ms b/a",
              "median ms b/a"));
      for (int i = 0; i < cases.size(); i++) {
        Measurement before = baseline.get(i);
        Measurement after = indexed.get(i);
        report.append(
            String.format(
                "%-26s %,12d -> %,-9d %,12d -> %,-12d %6d -> %-9d %6d -> %-6d%n",
                cases.get(i)[0],
                before.recordsRead,
                after.recordsRead,
                before.bytesRead,
                after.bytesRead,
                before.firstMs,
                after.firstMs,
                before.medianMs,
                after.medianMs));
      }
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    LOG.warn("\n{}", report);
    writeReport(scenario, report.toString());
    return indexed;
  }

  /** Runs a query once and returns its rows (used both for correctness and warmup). */
  private List<Row> lookupRows(String query) {
    return spark.sql(query).collectAsList();
  }

  /**
   * First-run millis and rows, median millis over the remaining runs (the first is a warmup), plus
   * input metrics from the final run.
   */
  private Measurement measure(String query, InputMetricsListener listener)
      throws InterruptedException {
    long[] samples = new long[RUNS];
    List<Row> rows = null;
    long bytesRead = 0;
    long recordsRead = 0;
    for (int run = 0; run < RUNS; run++) {
      listener.reset();
      long start = System.nanoTime();
      List<Row> result = lookupRows(query);
      samples[run] = (System.nanoTime() - start) / 1_000_000L;
      if (run == 0) {
        rows = result;
      }

      // Task-end events reach the listener asynchronously; give the bus a moment to drain.
      Thread.sleep(500);
      bytesRead = listener.bytesRead.get();
      recordsRead = listener.recordsRead.get();
    }
    long[] timed = java.util.Arrays.copyOfRange(samples, 1, RUNS); // drop the warmup (first) run
    java.util.Arrays.sort(timed);
    long median = timed[timed.length / 2];
    return new Measurement(recordsRead, bytesRead, samples[0], median, rows);
  }

  private void writeReport(String scenario, String report) throws IOException {
    Path path =
        Paths.get("build", "scalar-index-benchmark-" + catalogName + "-" + scenario + ".txt");
    Files.createDirectories(path.getParent());
    Files.write(path, report.getBytes(StandardCharsets.UTF_8));
    LOG.warn("Benchmark report written to {}", path.toAbsolutePath());
  }

  private static final class Measurement {
    private final long recordsRead;
    private final long bytesRead;
    private final long firstMs;
    private final long medianMs;
    private final List<Row> rows;

    Measurement(long recordsRead, long bytesRead, long firstMs, long medianMs, List<Row> rows) {
      this.recordsRead = recordsRead;
      this.bytesRead = bytesRead;
      this.firstMs = firstMs;
      this.medianMs = medianMs;
      this.rows = rows;
    }
  }

  /** Accumulates task input metrics (bytes/records read) across a query's tasks. */
  private static final class InputMetricsListener extends SparkListener {
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong recordsRead = new AtomicLong();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.taskMetrics() != null && taskEnd.taskMetrics().inputMetrics() != null) {
        bytesRead.addAndGet(taskEnd.taskMetrics().inputMetrics().bytesRead());
        recordsRead.addAndGet(taskEnd.taskMetrics().inputMetrics().recordsRead());
      }
    }

    void reset() {
      bytesRead.set(0);
      recordsRead.set(0);
    }
  }
}
