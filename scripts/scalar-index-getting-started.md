# SCALAR Index on Spark 4.0 — Getting Started

Steps to get the branch, build it, create a SCALAR index, query it, and benchmark it. The SCALAR
index maps a key to the data file and row position that holds it, so lookups on a non-partition
column can skip every file that cannot match. It is exact (no false positives, unlike a Bloom
filter): a key that is not in the index plans no covered files.

## 1. Get the code

```
git remote add osscm https://github.com/osscm/iceberg.git
git fetch osscm scalar_sec_index_spark40
git checkout -b scalar_sec_index_spark40 osscm/scalar_sec_index_spark40
```

The branch is stacked on the base `scalar_sec_index` branch, so compare against that, not `main`
(the fork's `main` is far behind upstream and a diff against it is mostly unrelated history).

A build needs at least these commits on the branch:

| Commit | What it provides |
|---|---|
| `25c5dda2a` | build procedure and scan pruning for Spark 4.0 |
| `9693f6d48` | a lookup with no index match plans no files (absent keys) |
| `e8d38c860` | driver-side caching of index metadata |

## 2. Build

Use JDK 17 or 21 (the build rejects other versions).

```
export JAVA_HOME=<path to a JDK 17 or 21>
./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
  :iceberg-spark:iceberg-spark-runtime-4.0_2.13:shadowJar
```

Put the resulting runtime jar on the Spark classpath and enable the Iceberg extensions and a catalog
as usual.

## 3. Build an index

```sql
CALL <catalog>.system.build_scalar_index(
  table => 'db.tbl',
  columns => array('id'),
  transform => 'IDENTITY');
```

| Transform | Key columns | Notes |
|---|---|---|
| `IDENTITY` | exactly one, long or int | point, `IN` and range predicates |
| `HASH` | exactly one | point and `IN` predicates |
| `ZORDER` | several | composite key. **Not supported on 4.0**: the build procedure's own documentation lists composite indexes as a non-goal, and the 4.0 Spark code has no composite query-time pruning. The Spark 3.5 branch (`scalar_sec_index`) has composite build and resolve. Do not expect multi-column lookups to be pruned on 4.0. |

The build is a full rebuild by default, so budget build time on large tables.
Optional `options => map(...)`:

| Option | Default | Effect |
|---|---|---|
| `mode` | `full` | `incremental` builds leaf files only for data files added since the existing index's last snapshot and appends them; it falls back to a full rebuild if there is no index to extend or the added files cannot be determined (for example after a compaction) |
| `target-leaf-files` | `4` | number of leaf files the index is split into |
| `leaf.row-group-size-bytes` | 1 MB | leaf-file Parquet row-group size; the main read-latency lever |

## 4. Query

No flags are needed. A query with an equality, `IN` or (for `IDENTITY`) range predicate on the
indexed column is resolved through the index automatically. Files added after the index was built
are always read, because the index does not cover them; rebuild the index to cover them.

The scan falls back to normal planning, with an INFO log line saying why, when there is no usable
index, the predicate is not eligible, a non-append snapshot happened since the index was built, the
key type does not match, or the index resolves to more candidate leaf files than the bound below.

## 5. Settings

Nothing has to be enabled: the caches below are on by default.

| Setting | Where | Default | Effect |
|---|---|---|---|
| `spark.sql.iceberg.scalar-index.listing-cache-ttl-ms` | Spark session conf | `30000` | how long a table's index listing is reused on the driver; `0` disables. An index built by another process can take up to this long to be seen. |
| `scalar-index.max-candidate-leaf-files` | table property | `100` | planning-cost bound; above it the scan falls back to normal planning |

Tracking-file entries, stored key types and leaf-file footers are cached on the driver by path,
with no time limit because those files are immutable. They are size-bounded (about 1M tracking
entries and about 256 MB of footers) and these bounds are constants, not settings.

## 6. Benchmark

`TestScalarIndexBenchmark` builds an index on a synthetic 10M-row, 200-file table and reports
records read, bytes read, first-run and median latency, before and after the index. It is skipped
unless `SCALAR_BENCHMARK=true` is set:

```
SCALAR_BENCHMARK=true ./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= \
  -DkafkaVersions= :iceberg-spark:iceberg-spark-extensions-4.0_2.13:test --rerun \
  --tests "*TestScalarIndexBenchmark.benchmarkAbsentKey"
```

Scenarios: `benchmarkUnpartitioned`, `benchmarkPartitioned`, `benchmarkAbsentKey`,
`benchmarkAbsentKeyWithUncoveredFile` and `benchmarkUnpartitionedListingCacheOff`. The class also
contains `benchmarkComposite`, copied from the 3.5 branch; it has not been run on 4.0 and composite
indexes are not supported there, so skip it. Reports are written to
`spark/v4.0/spark-extensions/build/scalar-index-benchmark-<catalog>-<scenario>.txt`.

Records read is the most reliable signal. The harness runs on local disk with a warm cache, so it
understates what the caches save against an object store, and wall-clock numbers vary between runs.
