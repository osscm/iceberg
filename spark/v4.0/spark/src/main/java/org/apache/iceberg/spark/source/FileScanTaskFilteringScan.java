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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchScan} decorator that constrains {@link #planFiles()} to the data files a SCALAR
 * index lookup resolved: the covered files with matching index entries plus the uncovered files
 * added after the index's source snapshot. When both sets are empty, no file can contain a matching
 * row and {@link #planFiles()} returns no tasks.
 *
 * <p>Only {@link #planFiles()} is restricted; Spark's task planning ({@link
 * SparkPartitioningAwareScan}) calls only {@link #planFiles()}. {@link #planTasks()} delegates
 * unfiltered, which only costs a missed optimization because the original predicate is still
 * applied as a residual.
 *
 * <p>The matched paths were recorded by the index build through Spark's {@code input_file_name()},
 * while planning reports {@link org.apache.iceberg.ContentFile#location()}. If the index matched
 * files but none of them is among the planned files, the paths are assumed not to be comparable and
 * the unfiltered file list is returned rather than risk dropping rows. Uncovered paths come from
 * the table's own manifests.
 *
 * <p>Every other method delegates to the wrapped scan without re-wrapping the result; refinement
 * happens before {@link SparkScanBuilder} wraps the scan.
 */
class FileScanTaskFilteringScan implements BatchScan {

  private static final Logger LOG = LoggerFactory.getLogger(FileScanTaskFilteringScan.class);

  private final BatchScan delegate;
  private final Set<String> matchedFilePaths;
  private final Set<String> uncoveredFilePaths;

  FileScanTaskFilteringScan(
      BatchScan delegate, Set<String> matchedFilePaths, Set<String> uncoveredFilePaths) {
    Preconditions.checkArgument(delegate != null, "Invalid delegate scan: null");
    Preconditions.checkArgument(matchedFilePaths != null, "Invalid matched file paths: null");
    Preconditions.checkArgument(uncoveredFilePaths != null, "Invalid uncovered file paths: null");
    this.delegate = delegate;
    this.matchedFilePaths = matchedFilePaths;
    this.uncoveredFilePaths = uncoveredFilePaths;
  }

  @Override
  public CloseableIterable<ScanTask> planFiles() {
    if (matchedFilePaths.isEmpty() && uncoveredFilePaths.isEmpty()) {
      LOG.info("SCALAR index found no data file that can match, planning no tasks");
      return CloseableIterable.empty();
    }

    List<ScanTask> allTasks = Lists.newArrayList();
    List<ScanTask> keptTasks = Lists.newArrayList();
    boolean anyMatchedFilePlanned = false;
    try (CloseableIterable<ScanTask> tasks = delegate.planFiles()) {
      for (ScanTask task : tasks) {
        allTasks.add(task);
        String location =
            task instanceof FileScanTask ? ((FileScanTask) task).file().location() : null;
        boolean matched = location != null && matchedFilePaths.contains(location);
        anyMatchedFilePlanned |= matched;
        if (matched || (location != null && uncoveredFilePaths.contains(location))) {
          keptTasks.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close planned file scan tasks", e);
    }

    if (!matchedFilePaths.isEmpty() && !anyMatchedFilePlanned && !allTasks.isEmpty()) {
      LOG.warn(
          "None of the {} data file(s) matched by the SCALAR index is among the {} planned file(s),"
              + " falling back to the unfiltered file set",
          matchedFilePaths.size(),
          allTasks.size());
      return CloseableIterable.withNoopClose(allTasks);
    }

    return CloseableIterable.withNoopClose(keptTasks);
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return delegate.planTasks();
  }

  @Override
  public Table table() {
    return delegate.table();
  }

  @Override
  public BatchScan useSnapshot(long snapshotId) {
    return delegate.useSnapshot(snapshotId);
  }

  @Override
  public BatchScan useRef(String ref) {
    return delegate.useRef(ref);
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    return delegate.asOfTime(timestampMillis);
  }

  @Override
  public Snapshot snapshot() {
    return delegate.snapshot();
  }

  @Override
  public BatchScan option(String property, String value) {
    return delegate.option(property, value);
  }

  @Override
  public BatchScan project(Schema schema) {
    return delegate.project(schema);
  }

  @Override
  public BatchScan caseSensitive(boolean caseSensitive) {
    return delegate.caseSensitive(caseSensitive);
  }

  @Override
  public boolean isCaseSensitive() {
    return delegate.isCaseSensitive();
  }

  @Override
  public BatchScan includeColumnStats() {
    return delegate.includeColumnStats();
  }

  @Override
  public BatchScan includeColumnStats(Collection<String> requestedColumns) {
    return delegate.includeColumnStats(requestedColumns);
  }

  @Override
  public BatchScan select(Collection<String> columns) {
    return delegate.select(columns);
  }

  @Override
  public BatchScan filter(Expression expr) {
    return delegate.filter(expr);
  }

  @Override
  public Expression filter() {
    return delegate.filter();
  }

  @Override
  public BatchScan ignoreResiduals() {
    return delegate.ignoreResiduals();
  }

  @Override
  public BatchScan planWith(ExecutorService executorService) {
    return delegate.planWith(executorService);
  }

  @Override
  public Schema schema() {
    return delegate.schema();
  }

  @Override
  public long targetSplitSize() {
    return delegate.targetSplitSize();
  }

  @Override
  public int splitLookback() {
    return delegate.splitLookback();
  }

  @Override
  public long splitOpenFileCost() {
    return delegate.splitOpenFileCost();
  }

  @Override
  public BatchScan metricsReporter(MetricsReporter reporter) {
    return delegate.metricsReporter(reporter);
  }

  @Override
  public BatchScan minRowsRequested(long numRows) {
    return delegate.minRowsRequested(numRows);
  }

  @Override
  public Supplier<FileIO> fileIO() {
    return delegate.fileIO();
  }
}
