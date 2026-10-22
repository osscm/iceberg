# SCALAR index on Spark 4.0 - git, build, test and benchmark commands

Fork: https://github.com/osscm/iceberg  (public)
Branch: scalar_sec_index_spark40  (tip 0bf17b227 or later)
Getting-started doc on the branch:
https://github.com/osscm/iceberg/blob/scalar_sec_index_spark40/scripts/scalar-index-getting-started.md

JDK 17 or 21 is required (the build rejects other versions).


## 1. Get and verify the branch

    git clone https://github.com/osscm/iceberg.git iceberg-scalar && cd iceberg-scalar
    # or, in an existing clone:
    #   git remote add osscm https://github.com/osscm/iceberg.git && git fetch osscm

    git fetch origin scalar_sec_index_spark40
    git checkout -b scalar_sec_index_spark40 origin/scalar_sec_index_spark40

    git log --oneline -8
    for c in 25c5dda2a 9693f6d48 e8d38c860; do
      git merge-base --is-ancestor $c HEAD && echo "$c: present" || echo "$c: MISSING"
    done

All three commits must print "present":
  25c5dda2a  build procedure and scan pruning for Spark 4.0
  9693f6d48  a lookup with no index match plans no files (absent keys)
  e8d38c860  driver-side caching of index metadata

Compare against the base branch scalar_sec_index, not main. The fork's main is far behind
upstream, so a diff against it is mostly unrelated history.


## 2. Build

    export JAVA_HOME=<path to a JDK 17 or 21>

    # runtime jar for Spark 4.0 (standard Iceberg task)
    ./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
      :iceberg-spark:iceberg-spark-runtime-4.0_2.13:shadowJar


## 3. Correctness tests

    ./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
      :iceberg-spark:iceberg-spark-extensions-4.0_2.13:test --rerun \
      --tests "*TestScalarIndexScanPruning" --tests "*TestBuildScalarIndexProcedure"

Expected: 100 tests (40 + 60), 0 failures, about 1.5 minutes.

Cache unit tests (module iceberg-spark-4.0_2.13):

    ./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
      :iceberg-spark:iceberg-spark-4.0_2.13:test --rerun \
      --tests "*TestIndexListingCache" --tests "*TestFooterCachingInputFile" \
      --tests "*TestScalarIndexFileResolver"

Expected: 33 tests (7 + 7 + 19), 0 failures.


## 4. Benchmark

The benchmark class is skipped unless SCALAR_BENCHMARK=true is set.

Environment variables:
  SCALAR_BENCHMARK       must be true, or the class is skipped
  SCALAR_BENCH_ROWS      row count, default 10000000 (underscores allowed)
  SCALAR_BENCH_FILES     number of data files, default 200
  SCALAR_BENCH_CATALOG   run one catalog only (e.g. testhadoop); all four run when unset

His query shapes at 30M rows and 643 files, one catalog:

    SCALAR_BENCHMARK=true SCALAR_BENCH_CATALOG=testhadoop \
    SCALAR_BENCH_ROWS=30000000 SCALAR_BENCH_FILES=643 \
    ./gradlew -DsparkVersions=4.0 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
      :iceberg-spark:iceberg-spark-extensions-4.0_2.13:test --rerun \
      --tests "*TestScalarIndexBenchmark.benchmarkColleagueSeries"

    cat spark/v4.0/spark-extensions/build/scalar-index-benchmark-testhadoop-colleague-series.txt

Check that the report header reads "rows=30000000 files=643" and that gradlew exits 0.
The 30M run took about 3 minutes on a laptop. Drop the ROWS and FILES variables for the
default 10M run. Each run overwrites the previous report for the same catalog and scenario,
so copy it aside first.

Other scenarios (change the --tests name):
  benchmarkAbsentKey
  benchmarkAbsentKeyWithUncoveredFile
  benchmarkUnpartitioned
  benchmarkPartitioned
  benchmarkUnpartitionedListingCacheOff
  benchmarkColleagueSeries

Do not run benchmarkComposite: composite indexes are not supported on 4.0.


## 5. Settings (nothing needs to be enabled)

  spark.sql.iceberg.scalar-index.listing-cache-ttl-ms   Spark session conf, default 30000.
      How long a table's index listing is reused on the driver. 0 disables it.
  scalar-index.max-candidate-leaf-files                 table property, default 100.
      Above this many candidate leaf files the scan falls back to normal planning.

The tracking-file, key-type and footer caches are always on and have no settings. They live
in the driver JVM, so use one long-lived session for the whole query set. A fresh driver per
query starts cold every time.


## 6. Reading the benchmark report

Columns: records read, bytes read, first ms, median ms, each as "no index -> with index".
Records read is the reliable signal. "first ms" is the cold first run. The harness runs on
local disk with a warm cache, so wall-clock numbers vary between runs and understate what
the caches save on an object store.
