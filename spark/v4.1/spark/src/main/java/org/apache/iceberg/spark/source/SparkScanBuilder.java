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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SparkDistributedDataScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileReader;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileReader;
import org.apache.iceberg.index.TransformFunction;
import org.apache.iceberg.index.TransformFunctions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.IndexSnapshotUtil;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkAggregates;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkScanBuilder extends BaseSparkScanBuilder
    implements SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit,
        SupportsPushDownAggregates {

  private static final Logger LOG = LoggerFactory.getLogger(SparkScanBuilder.class);

  private final Snapshot snapshot;
  private final String branch;
  private final TimeTravel timeTravel;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private Scan localScan;
  private Set<String> scalarIndexResolvedFilePaths = null;

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this(
        spark,
        table,
        table.schema(),
        table.currentSnapshot(),
        null /* no branch */,
        null /* no time travel */,
        options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      CaseInsensitiveStringMap options) {
    this(spark, table, schema, snapshot, branch, null /* no time travel */, options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      TimeTravel timeTravel,
      CaseInsensitiveStringMap options) {
    super(spark, table, schema, options);
    this.snapshot = snapshot;
    this.branch = branch;
    this.timeTravel = timeTravel;
    if (Spark3Util.containsIncrementalOptions(options)) {
      Preconditions.checkArgument(timeTravel == null, "Cannot use time travel in incremental scan");
      Pair<Long, Long> boundaries = readConf().incrementalAppendScanBoundaries();
      this.startSnapshotId = boundaries.first();
      this.endSnapshotId = boundaries.second();
    } else {
      this.startSnapshotId = null;
      this.endSnapshotId = null;
    }
    Spark3Util.validateNoLegacyTimeTravel(options);
    SparkTableUtil.validateReadBranch(spark, table, branch, options);
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    Predicate[] postScanPredicates = super.pushPredicates(predicates);
    this.scalarIndexResolvedFilePaths = null;
    tryPruneUsingScalarIndex();
    return postScanPredicates;
  }

  private static final Set<Expression.Operation> SCALAR_INDEX_PRUNABLE_OPS =
      ImmutableSet.of(
          Expression.Operation.EQ,
          Expression.Operation.IN,
          Expression.Operation.LT,
          Expression.Operation.LT_EQ,
          Expression.Operation.GT,
          Expression.Operation.GT_EQ);

  // Open Question 2 in the design doc: without a bound, a misconfigured index (e.g. a very large
  // hash.num-buckets, or a wide IN list) could resolve to more candidate leaf files than it would
  // ever be worth opening -- planning could end up slower than no index at all. Overridable per
  // table via the scalar-index.max-candidate-leaf-files property, read below.
  private static final String MAX_CANDIDATE_LEAF_FILES_PROPERTY =
      "scalar-index.max-candidate-leaf-files";
  private static final int DEFAULT_MAX_CANDIDATE_LEAF_FILES = 100;

  /**
   * If a SCALAR index exists on a column referenced by an equality, {@code IN}, or range predicate
   * in {@link #filters()}, resolves the predicate(s) against the index's leaf files and records the
   * matching source file paths in {@link #scalarIndexResolvedFilePaths}, so {@link #buildBatchScan}
   * can constrain the scan to just those files via {@link FileScanTaskFilteringScan}.
   *
   * <p>An equality predicate (HASH or IDENTITY transform) resolves to a single transform value. An
   * {@code IN} predicate resolves to one transform value per literal (HASH or IDENTITY), queried as
   * separate points rather than a combined range -- HASH in particular can scatter an IN-list's
   * values across unrelated, non-contiguous buckets, so there is no single [min, max] that would be
   * both correct and useful. A range predicate ({@code <}, {@code <=}, {@code >}, {@code >=} --
   * including a {@code BETWEEN}, which Spark decomposes into two range predicates on the same
   * column) only makes sense against an IDENTITY-transform index: HASH scatters values across
   * buckets, so a contiguous range on the original column does not map to a contiguous range of
   * transform values the way it does for IDENTITY, where the transform value is the key value
   * itself.
   *
   * <p>Restricting the scan to files that could satisfy one AND'd predicate (or set of predicates
   * on the same column) is always sound: any row satisfying the full pushed-down conjunction must
   * also satisfy it, so it must live in one of these files. The predicate itself is still pushed
   * down and applied as a residual regardless, so a wrong or stale resolution here can only miss an
   * optimization, never produce a wrong result -- except for the zero-match case (key confirmed
   * absent from a fully fresh index, with no uncovered files), which is deliberately NOT pruned to
   * zero files here: that would be a correctness-sensitive optimization (a bug would silently
   * return wrong empty results, not just miss a speedup), left as a documented follow-up rather
   * than attempted in this pass.
   *
   * <p>Staleness is handled via the covered/uncovered-files model from Huaxin Gao's Primary Key
   * Index for Apache Iceberg proposal (Section 8, "Staleness Semantics"): files that existed at the
   * index's own snapshot ("covered") can be pruned using the index as usual; files added to the
   * table since ("uncovered") are never known to the index and are always included in the resolved
   * set, unconditionally. This lets a stale index still help, rather than falling back to no
   * pruning at all on any snapshot mismatch. See {@link #uncoveredFilePathsSince}.
   *
   * <p>Only the first column whose predicate(s) resolve against an existing index is used;
   * combining resolutions from multiple SCALAR indexes on an AND'd query would need set
   * intersection across indexes, which is not yet attempted.
   *
   * <p>Any failure -- no index registered, an unsupported predicate shape, a non-append snapshot
   * (e.g. compaction) between the index's snapshot and the current one, an I/O error reading the
   * tracking or leaf file -- falls back silently to normal planning, matching the design proposal's
   * rule that the index must never be required for correctness.
   */
  private void tryPruneUsingScalarIndex() {
    if (!isMainTable() || startSnapshotId != null || filters().isEmpty()) {
      return;
    }

    Map<String, List<UnboundPredicate<?>>> candidatesByColumn = candidatePredicatesByColumn();
    if (candidatesByColumn.isEmpty()) {
      return;
    }

    // Derived the same way BuildScalarIndexProcedure derives it (TableIdentifier.parse of the
    // core Table's own name), not from the Spark catalog Identifier -- the two must produce an
    // identical TableIdentifier or listIndexes below would silently see a disjoint set.
    TableIdentifier tableIdentifier = TableIdentifier.parse(table().name());
    List<IndexMetadata> availableIndexes;
    try {
      availableIndexes = SparkIndexCatalogs.get().catalogFor(table()).listIndexes(tableIdentifier);
    } catch (Exception e) {
      LOG.warn(
          "Failed to list SCALAR indexes for table {}, falling back to normal planning: {}",
          tableIdentifier,
          e.getMessage());
      return;
    }

    if (availableIndexes.isEmpty()) {
      return;
    }

    // Try composite (multi-column) indexes first, then single-column indexes per constrained
    // column.
    if (tryPruneUsingCompositeIndexes(availableIndexes, candidatesByColumn)) {
      return;
    }

    for (Map.Entry<String, List<UnboundPredicate<?>>> entry : candidatesByColumn.entrySet()) {
      if (tryPruneUsingScalarIndex(availableIndexes, entry.getKey(), entry.getValue())) {
        return;
      }
    }
  }

  /** Groups the pushable predicates (EQ/IN/range on a named column) by column name. */
  private Map<String, List<UnboundPredicate<?>>> candidatePredicatesByColumn() {
    Map<String, List<UnboundPredicate<?>>> candidatesByColumn = Maps.newLinkedHashMap();
    for (Expression expr : filters()) {
      if (!(expr instanceof UnboundPredicate) || !SCALAR_INDEX_PRUNABLE_OPS.contains(expr.op())) {
        continue;
      }

      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      if (!(predicate.term() instanceof NamedReference)) {
        continue;
      }

      String columnName = ((NamedReference<?>) predicate.term()).name();
      candidatesByColumn.computeIfAbsent(columnName, c -> Lists.newArrayList()).add(predicate);
    }
    return candidatesByColumn;
  }

  /** Tries each composite (multi-column) SCALAR index whose key columns are all constrained. */
  private boolean tryPruneUsingCompositeIndexes(
      List<IndexMetadata> availableIndexes,
      Map<String, List<UnboundPredicate<?>>> candidatesByColumn) {
    for (IndexMetadata metadata : availableIndexes) {
      if ("SCALAR".equals(metadata.type())
          && metadata.keyColumnIds().size() > 1
          && tryPruneUsingCompositeIndex(metadata, candidatesByColumn)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Attempts to resolve {@code predicates} (all on {@code columnName}, each {@code EQ}, {@code IN},
   * or a range comparison) against a SCALAR index covering that column, discovered from {@code
   * availableIndexes} (as returned by {@link IndexCatalog#listIndexes}) by matching the column's
   * field ID against {@link IndexMetadata#keyColumnIds()} -- not by any naming convention. Returns
   * {@code true} if it resolved and set {@link #scalarIndexResolvedFilePaths}, {@code false} to let
   * {@link #tryPruneUsingScalarIndex()} try the next column's predicates instead.
   */
  private boolean tryPruneUsingScalarIndex(
      List<IndexMetadata> availableIndexes,
      String columnName,
      List<UnboundPredicate<?>> predicates) {
    Types.NestedField keyField = schema().findField(columnName);
    if (keyField == null) {
      return false;
    }

    IndexMetadata metadata =
        availableIndexes.stream()
            .filter(m -> "SCALAR".equals(m.type()))
            .filter(m -> m.keyColumnIds().contains(keyField.fieldId()))
            .findFirst()
            .orElse(null);
    if (metadata == null || snapshot == null) {
      return false;
    }

    try {
      IndexSnapshot indexSnapshot = metadata.currentSnapshot();
      if (indexSnapshot == null) {
        return false;
      }

      // Read the tracking file once and reuse the entries for both the type-promotion check and
      // candidate collection, rather than re-reading (and re-decoding) it per use and per range.
      List<TrackingFileEntry> trackedLeafFiles =
          TrackingFileReader.readAll(table().io().newInputFile(indexSnapshot.trackingFile()));

      if (keyTypePromoted(trackedLeafFiles, ImmutableList.of(keyField), columnName)) {
        return false;
      }

      long readSnapshotId = snapshot.snapshotId();
      Set<String> uncoveredFilePaths = ImmutableSet.of();
      if (indexSnapshot.sourceTableSnapshotId() != readSnapshotId) {
        // Stale relative to the snapshot being read -- rather than fall back entirely,
        // find the files added since the index's snapshot (uncovered) so covered files can
        // still be pruned via the index. Throws if the index's snapshot isn't a clean append
        // ancestor of the current one (e.g. a compaction ran in between); the outer catch
        // below falls back to no pruning at all in that case, same as before this change.
        uncoveredFilePaths =
            uncoveredFilePathsSince(indexSnapshot.sourceTableSnapshotId(), readSnapshotId);
      }

      ResolvedPredicate resolved = resolvePredicates(metadata, columnName, predicates);
      if (resolved == null) {
        return false;
      }

      return resolveAndSetFilePaths(
          trackedLeafFiles, resolved, ImmutableList.of(keyField), columnName, uncoveredFilePaths);
    } catch (Exception e) {
      LOG.warn(
          "Failed to use SCALAR index on column {}, falling back to normal planning: {}",
          columnName,
          e.getMessage());
      return false;
    }
  }

  /**
   * Attempts to resolve a composite (multi-column) SCALAR index: every key column must be
   * constrained by the query. Builds a single bounding transform-value range from the predicate
   * box's corners -- correct because the composite transform (Z-order) is monotonic under the
   * componentwise product order, so every matching key's transform value lies within it -- and
   * applies the AND of the per-column predicates as the exact residual re-check.
   */
  private boolean tryPruneUsingCompositeIndex(
      IndexMetadata metadata, Map<String, List<UnboundPredicate<?>>> candidatesByColumn) {
    if (snapshot == null) {
      return false;
    }

    List<Types.NestedField> keyFields =
        Lists.newArrayListWithExpectedSize(metadata.keyColumnIds().size());
    List<String> columnNames = Lists.newArrayList();
    List<Object> lowerCorner = Lists.newArrayList();
    List<Object> upperCorner = Lists.newArrayList();
    List<Expression> residualPredicates = Lists.newArrayList();
    for (int fieldId : metadata.keyColumnIds()) {
      Types.NestedField keyField = schema().findField(fieldId);
      if (keyField == null) {
        return false;
      }
      List<UnboundPredicate<?>> predicates = candidatesByColumn.get(keyField.name());
      if (predicates == null || predicates.isEmpty()) {
        return false;
      }
      ColumnBound bound = columnBound(predicates);
      if (bound == null) {
        return false;
      }
      keyFields.add(keyField);
      columnNames.add(keyField.name());
      lowerCorner.add(bound.lower);
      upperCorner.add(bound.upper);
      residualPredicates.addAll(predicates);
    }

    String label = String.join(",", columnNames);
    try {
      IndexSnapshot indexSnapshot = metadata.currentSnapshot();
      if (indexSnapshot == null) {
        return false;
      }

      List<TrackingFileEntry> trackedLeafFiles =
          TrackingFileReader.readAll(table().io().newInputFile(indexSnapshot.trackingFile()));
      if (keyTypePromoted(trackedLeafFiles, keyFields, label)) {
        return false;
      }

      long readSnapshotId = snapshot.snapshotId();
      Set<String> uncoveredFilePaths = ImmutableSet.of();
      if (indexSnapshot.sourceTableSnapshotId() != readSnapshotId) {
        uncoveredFilePaths =
            uncoveredFilePathsSince(indexSnapshot.sourceTableSnapshotId(), readSnapshotId);
      }

      TransformFunction transform =
          TransformFunctions.forName(metadata.transformFunction(), metadata.properties());
      long cornerA = transform.transform(lowerCorner);
      long cornerB = transform.transform(upperCorner);
      TransformValueRange bounding =
          new TransformValueRange(Math.min(cornerA, cornerB), Math.max(cornerA, cornerB));
      ResolvedPredicate resolved =
          new ResolvedPredicate(
              ImmutableList.of(bounding), combinePredicates(residualPredicates), columnNames);
      return resolveAndSetFilePaths(
          trackedLeafFiles, resolved, keyFields, label, uncoveredFilePaths);
    } catch (Exception e) {
      LOG.warn(
          "Failed to use composite SCALAR index on {}, falling back to normal planning: {}",
          label,
          e.getMessage());
      return false;
    }
  }

  /** The bounded [lower, upper] value range for one key column of a composite query box. */
  private static final class ColumnBound {
    private final Object lower;
    private final Object upper;

    ColumnBound(Object lower, Object upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  /**
   * Derives a bounded {@code [lower, upper]} for one key column from its predicates, or {@code
   * null} if the column is not bounded on both sides (so a finite box corner cannot be formed).
   */
  private static ColumnBound columnBound(List<UnboundPredicate<?>> predicates) {
    ColumnBound eq = equalityBound(predicates);
    if (eq != null) {
      return eq;
    }
    ColumnBound in = inBound(predicates);
    if (in != null) {
      return in;
    }
    return rangeBound(predicates);
  }

  private static ColumnBound equalityBound(List<UnboundPredicate<?>> predicates) {
    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.EQ) {
        Object value = predicate.literal().value();
        return new ColumnBound(value, value);
      }
    }
    return null;
  }

  private static ColumnBound inBound(List<UnboundPredicate<?>> predicates) {
    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() != Expression.Operation.IN) {
        continue;
      }
      Object min = null;
      Object max = null;
      for (Literal<?> literal : predicate.literals()) {
        Object value = literal.value();
        if (min == null || compareValues(value, min) < 0) {
          min = value;
        }
        if (max == null || compareValues(value, max) > 0) {
          max = value;
        }
      }
      return min == null ? null : new ColumnBound(min, max);
    }
    return null;
  }

  private static ColumnBound rangeBound(List<UnboundPredicate<?>> predicates) {
    Object lower = null;
    Object upper = null;
    for (UnboundPredicate<?> predicate : predicates) {
      switch (predicate.op()) {
        case GT:
        case GT_EQ:
          lower = predicate.literal().value();
          break;
        case LT:
        case LT_EQ:
          upper = predicate.literal().value();
          break;
        default:
          break;
      }
    }
    return (lower == null || upper == null) ? null : new ColumnBound(lower, upper);
  }

  @SuppressWarnings("unchecked")
  private static int compareValues(Object left, Object right) {
    return ((Comparable<Object>) left).compareTo(right);
  }

  private static Expression combinePredicates(List<Expression> predicates) {
    Expression combined = null;
    for (Expression predicate : predicates) {
      combined = combined == null ? predicate : Expressions.and(combined, predicate);
    }
    return combined;
  }

  /**
   * Collects the candidate leaf files for {@code resolved}, enforces the planning-cost bound, reads
   * the matching entries, and sets {@link #scalarIndexResolvedFilePaths} to the covered match files
   * plus any uncovered files. Returns {@code true} if pruning was applied, {@code false} to fall
   * back to normal planning.
   */
  private boolean resolveAndSetFilePaths(
      List<TrackingFileEntry> trackedLeafFiles,
      ResolvedPredicate resolved,
      List<Types.NestedField> keyFields,
      String label,
      Set<String> uncoveredFilePaths) {
    // Dedupe by location: an IN predicate's separate target ranges can resolve to the same leaf
    // file (e.g. two IN values landing in the same HASH bucket), and reading it twice would just
    // waste work, not affect correctness.
    List<TrackingFileEntry> candidateLeafFiles =
        collectCandidateLeafFiles(trackedLeafFiles, resolved.targetRanges);

    int maxCandidateLeafFiles =
        PropertyUtil.propertyAsInt(
            table().properties(),
            MAX_CANDIDATE_LEAF_FILES_PROPERTY,
            DEFAULT_MAX_CANDIDATE_LEAF_FILES);
    if (candidateLeafFiles.size() > maxCandidateLeafFiles) {
      // Resolved to more candidate leaf files than it's worth opening -- bail out to normal
      // planning rather than let a misconfigured index make planning slower than no index at all.
      // The original predicate is still applied downstream regardless, so this is purely a missed
      // optimization, not a correctness concern.
      LOG.info(
          "SCALAR index on {} resolved to {} candidate leaf file(s), exceeding the planning-cost"
              + " bound of {} ({}) -- falling back to normal planning",
          label,
          candidateLeafFiles.size(),
          maxCandidateLeafFiles,
          MAX_CANDIDATE_LEAF_FILES_PROPERTY);
      return false;
    }

    List<LeafFileEntry> matches = Lists.newArrayList();
    for (TrackingFileEntry leaf : candidateLeafFiles) {
      matches.addAll(
          LeafFileReader.readMatching(
              table().io().newInputFile(leaf.location()), keyFields, resolved.leafFilter));
    }

    if (matches.isEmpty() && uncoveredFilePaths.isEmpty()) {
      // 0 matches (key not present in covered files) and no uncovered files either -- fall back to
      // normal planning rather than prune to zero; the original predicate itself still gets applied
      // downstream and yields no rows.
      return false;
    }

    // Uncovered files are unconditionally included regardless of what the index says -- they're not
    // covered by it, so they must always be scanned.
    Set<String> resolvedPaths = Sets.newHashSet(uncoveredFilePaths);
    matches.forEach(m -> resolvedPaths.add(m.filePath()));
    this.scalarIndexResolvedFilePaths = resolvedPaths;
    LOG.info(
        "SCALAR index on {} resolved {} to {} file(s) ({} covered match(es), {} uncovered"
            + " file(s)): {}",
        label,
        resolved.literalValueForLog,
        resolvedPaths.size(),
        matches.size(),
        uncoveredFilePaths.size(),
        resolvedPaths);
    return true;
  }

  private boolean keyTypePromoted(
      List<TrackingFileEntry> trackedLeafFiles, List<Types.NestedField> keyFields, String label) {
    // If a key column was promoted (e.g. int -> long) since the index was built, a transform value
    // computed from the current type no longer matches what is stored in the leaf files. On its own
    // that would just miss matches, but combined with a stale index (uncovered files present) it
    // could prune away covered rows that do match -- a wrong (empty) result. Detect it by comparing
    // the type physically stored in a leaf file against each current key type; the predicate itself
    // is still applied downstream, so results stay correct.
    if (trackedLeafFiles.isEmpty()) {
      return false;
    }

    InputFile leafFile = table().io().newInputFile(trackedLeafFiles.get(0).location());
    for (Types.NestedField keyField : keyFields) {
      Type storedKeyType = LeafFileReader.storedKeyType(leafFile, keyField);
      if (!keyField.type().equals(storedKeyType)) {
        LOG.warn(
            "SCALAR index on {} was built for key column {} of type {} but the current schema has "
                + "{} -- falling back to normal planning; rebuild the index to re-enable pruning",
            label,
            keyField.name(),
            storedKeyType,
            keyField.type());
        return true;
      }
    }
    return false;
  }

  private ResolvedPredicate resolvePredicates(
      IndexMetadata metadata, String columnName, List<UnboundPredicate<?>> predicates) {
    Optional<UnboundPredicate<?>> eqPredicate =
        predicates.stream().filter(p -> p.op() == Expression.Operation.EQ).findFirst();
    if (eqPredicate.isPresent()) {
      return resolveEqPredicate(metadata, columnName, eqPredicate.get());
    }

    Optional<UnboundPredicate<?>> inPredicate =
        predicates.stream().filter(p -> p.op() == Expression.Operation.IN).findFirst();
    if (inPredicate.isPresent()) {
      return resolveInPredicate(metadata, columnName, inPredicate.get());
    }

    return resolveRangePredicates(metadata, columnName, predicates);
  }

  private ResolvedPredicate resolveEqPredicate(
      IndexMetadata metadata, String columnName, UnboundPredicate<?> eqPredicate) {
    Object literalValue = eqPredicate.literal().value();
    long targetTransformValue = transformValue(metadata, literalValue);
    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(targetTransformValue, targetTransformValue)),
        Expressions.equal(columnName, literalValue),
        literalValue);
  }

  private ResolvedPredicate resolveInPredicate(
      IndexMetadata metadata, String columnName, UnboundPredicate<?> inPredicate) {
    List<Object> literalValues = Lists.newArrayList();
    for (Literal<?> literal : inPredicate.literals()) {
      literalValues.add(literal.value());
    }
    if (literalValues.isEmpty()) {
      return null;
    }

    List<TransformValueRange> ranges = Lists.newArrayListWithExpectedSize(literalValues.size());
    for (Object literalValue : literalValues) {
      long transformed = transformValue(metadata, literalValue);
      ranges.add(new TransformValueRange(transformed, transformed));
    }
    return new ResolvedPredicate(ranges, Expressions.in(columnName, literalValues), literalValues);
  }

  private ResolvedPredicate resolveRangePredicates(
      IndexMetadata metadata, String columnName, List<UnboundPredicate<?>> predicates) {
    // Range comparisons only map to a contiguous transform-value range for order-preserving
    // transforms (IDENTITY, Hilbert); HASH scatters values across buckets, so a range on the
    // original column tells us nothing about which buckets to look in.
    if (!TransformFunctions.forName(metadata.transformFunction(), metadata.properties())
        .supportsRangePredicates()) {
      return null;
    }

    long lowerBound = Long.MIN_VALUE;
    long upperBound = Long.MAX_VALUE;
    Expression combinedFilter = null;
    for (UnboundPredicate<?> predicate : predicates) {
      long value = ((Number) predicate.literal().value()).longValue();
      switch (predicate.op()) {
        case GT:
        case GT_EQ:
          // Deliberately loose (uses value, not value + 1, for GT): the coarse tracking-file range
          // only needs to be a superset of the true match set -- LeafFileReader re-applies the
          // exact original predicate below, so this can only cost scanning one extra boundary leaf
          // file, never under-prune a real match away.
          lowerBound = Math.max(lowerBound, value);
          break;
        case LT:
        case LT_EQ:
          upperBound = Math.min(upperBound, value);
          break;
        default:
          // EQ/IN are handled above; SCALAR_INDEX_PRUNABLE_OPS admits nothing else here.
          break;
      }
      combinedFilter =
          combinedFilter == null ? predicate : Expressions.and(combinedFilter, predicate);
    }

    if (combinedFilter == null || lowerBound > upperBound) {
      return null;
    }

    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(lowerBound, upperBound)),
        combinedFilter,
        "[" + lowerBound + ", " + upperBound + "]");
  }

  /**
   * The target transform-value ranges, the leaf-file filter, and a log label, resolved from a group
   * of predicates on one indexed column.
   */
  private static final class ResolvedPredicate {
    private final List<TransformValueRange> targetRanges;
    private final Expression leafFilter;
    private final Object literalValueForLog;

    ResolvedPredicate(
        List<TransformValueRange> targetRanges, Expression leafFilter, Object literalValueForLog) {
      this.targetRanges = targetRanges;
      this.leafFilter = leafFilter;
      this.literalValueForLog = literalValueForLog;
    }
  }

  /**
   * One [min, max] transform-value sub-range to query the tracking file for.
   *
   * <p>Package-private (not private), along with {@link #collectCandidateLeafFiles}, so both can be
   * tested directly against a real tracking file without needing a full Spark session -- see
   * TestSparkScanBuilderCandidateLeafFiles in this package.
   */
  static final class TransformValueRange {
    private final long min;
    private final long max;

    TransformValueRange(long min, long max) {
      this.min = min;
      this.max = max;
    }

    long min() {
      return min;
    }

    long max() {
      return max;
    }
  }

  /**
   * Collects the tracking-file entries whose transform-value range overlaps any of {@code
   * targetRanges}, deduped by location. An {@code IN} predicate's separate target ranges can
   * resolve to the same leaf file (e.g. two IN values landing in the same HASH bucket); reading it
   * twice would just waste work, not affect correctness, but is worth avoiding. Reads the tracking
   * file once; prefer {@link #collectCandidateLeafFiles(List, List)} when the entries are already
   * in hand.
   */
  static List<TrackingFileEntry> collectCandidateLeafFiles(
      FileIO io, String trackingFileLocation, List<TransformValueRange> targetRanges) {
    return collectCandidateLeafFiles(
        TrackingFileReader.readAll(io.newInputFile(trackingFileLocation)), targetRanges);
  }

  /** Filters already-read tracking entries by {@code targetRanges}, deduped by location. */
  static List<TrackingFileEntry> collectCandidateLeafFiles(
      List<TrackingFileEntry> trackedLeafFiles, List<TransformValueRange> targetRanges) {
    Map<String, TrackingFileEntry> byLocation = Maps.newLinkedHashMap();
    for (TransformValueRange range : targetRanges) {
      for (TrackingFileEntry entry : trackedLeafFiles) {
        // Overlap: entry range [lower, upper] overlaps [min, max] iff lower <= max AND upper >=
        // min.
        if (entry.transformValueLowerBound() <= range.max()
            && entry.transformValueUpperBound() >= range.min()) {
          byLocation.putIfAbsent(entry.location(), entry);
        }
      }
    }
    return Lists.newArrayList(byLocation.values());
  }

  private static long transformValue(IndexMetadata metadata, Object literalValue) {
    return TransformFunctions.forName(metadata.transformFunction(), metadata.properties())
        .transform(ImmutableList.of(literalValue));
  }

  /**
   * Data file paths added to {@link #table()} strictly after {@code sourceSnapshotId} up to and
   * including {@code currentSnapshotId} -- the "uncovered" files in the covered/uncovered-files
   * staleness model (see {@link #tryPruneUsingScalarIndex}). Delegates to {@link
   * IndexSnapshotUtil#addedFilePathsSince}, shared with the write side ({@code
   * BuildScalarIndexProcedure}'s incremental build path) so a fix to this correctness-sensitive
   * logic can't diverge between the two.
   */
  private Set<String> uncoveredFilePathsSince(long sourceSnapshotId, long currentSnapshotId) {
    return IndexSnapshotUtil.addedFilePathsSince(table(), sourceSnapshotId, currentSnapshotId);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (!canPushDownAggregation(aggregation)) {
      return false;
    }

    AggregateEvaluator aggregateEvaluator;
    List<BoundAggregate<?, ?>> expressions =
        Lists.newArrayListWithExpectedSize(aggregation.aggregateExpressions().length);

    for (AggregateFunc aggregateFunc : aggregation.aggregateExpressions()) {
      try {
        Expression expr = SparkAggregates.convert(aggregateFunc);
        if (expr != null) {
          Expression bound = Binder.bind(projection().asStruct(), expr, caseSensitive());
          expressions.add((BoundAggregate<?, ?>) bound);
        } else {
          LOG.info(
              "Skipping aggregate pushdown: AggregateFunc {} can't be converted to iceberg"
                  + " expression",
              aggregateFunc);
          return false;
        }
      } catch (IllegalArgumentException e) {
        LOG.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", aggregateFunc, e);
        return false;
      }
    }

    aggregateEvaluator = AggregateEvaluator.create(expressions);

    if (!metricsModeSupportsAggregatePushDown(aggregateEvaluator.aggregates())) {
      return false;
    }

    try (CloseableIterable<FileScanTask> fileScanTasks = planFilesWithStats()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        aggregateEvaluator.update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    if (!aggregateEvaluator.allAggregatorsValid()) {
      return false;
    }

    StructType pushedAggregateSchema =
        SparkSchemaUtil.convert(new Schema(aggregateEvaluator.resultType().fields()));
    InternalRow[] pushedAggregateRows = new InternalRow[1];
    StructLike structLike = aggregateEvaluator.result();
    pushedAggregateRows[0] =
        new StructInternalRow(aggregateEvaluator.resultType()).setStruct(structLike);
    localScan = new SparkLocalScan(table(), pushedAggregateSchema, pushedAggregateRows, filters());

    return true;
  }

  private boolean canPushDownAggregation(Aggregation aggregation) {
    if (!isMainTable()) {
      return false;
    }

    if (!readConf().aggregatePushDownEnabled()) {
      return false;
    }

    // If group by expression is the same as the partition, the statistics information can still
    // be used to calculate min/max/count, will enable aggregate push down in next phase.
    // TODO: enable aggregate push down for partition col group by expression
    if (aggregation.groupByExpressions().length > 0) {
      LOG.info("Skipping aggregate pushdown: group by aggregation push down is not supported");
      return false;
    }

    return true;
  }

  private boolean metricsModeSupportsAggregatePushDown(List<BoundAggregate<?, ?>> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table());
    for (BoundAggregate aggregate : aggregates) {
      String colName = aggregate.columnName();
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode instanceof MetricsModes.None) {
          LOG.info("Skipping aggregate pushdown: No metrics for column {}", colName);
          return false;
        } else if (mode instanceof MetricsModes.Counts) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from count for column {}",
                colName);
            return false;
          }
        } else if (aggregate.type().typeId() == Type.TypeID.STRING
            || aggregate.type().typeId() == Type.TypeID.BINARY) {
          // lower_bounds and upper_bounds may have been truncated before, so disable push down
          // regardless of the current mode
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from truncated values for"
                    + " column {}",
                colName);
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  public Scan build() {
    return build(true /* allow SCALAR index pruning */);
  }

  /** Builds the scan for merge-on-read row-level operations, which never use SCALAR indexes. */
  Scan buildMergeOnReadScan() {
    return build(false /* no SCALAR index pruning */);
  }

  private Scan build(boolean allowScalarIndexPruning) {
    if (localScan != null) {
      return localScan;
    } else if (startSnapshotId != null) {
      return buildIncrementalAppendScan();
    } else {
      return buildBatchScan(allowScalarIndexPruning);
    }
  }

  private Scan buildBatchScan(boolean allowScalarIndexPruning) {
    Schema projection = projectionWithMetadataColumns();
    BatchScan scan =
        buildIcebergBatchScan(projection, false /* use residuals */, false /* no stats */);
    return new SparkBatchQueryScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        allowScalarIndexPruning ? withScalarIndexPruning(scan) : scan,
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  // Scoped to the plain SELECT batch-scan path only -- incremental-append, changelog,
  // merge-on-read, and copy-on-write scans have different correctness considerations (e.g.
  // row-level operations may need to see files beyond ones matching a single equality predicate)
  // and are intentionally left untouched by this optimization.
  private BatchScan withScalarIndexPruning(BatchScan scan) {
    if (scan != null && scalarIndexResolvedFilePaths != null) {
      return new FileScanTaskFilteringScan(scan, scalarIndexResolvedFilePaths);
    }

    return scan;
  }

  private Scan buildIncrementalAppendScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkIncrementalAppendScan(
        spark(),
        table(),
        startSnapshotId,
        endSnapshotId,
        buildIcebergIncrementalAppendScan(projection, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  public Scan buildCopyOnWriteScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkCopyOnWriteScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        buildIcebergBatchScan(projection, true /* ignore residuals */, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private CloseableIterable<FileScanTask> planFilesWithStats() {
    Schema projection = projectionWithMetadataColumns();
    org.apache.iceberg.Scan<?, ?, ?> scan = buildIcebergScanWithStats(projection);
    if (scan != null) {
      return CloseableIterable.transform(scan.planFiles(), ScanTask::asFileScanTask);
    } else {
      return CloseableIterable.empty();
    }
  }

  private org.apache.iceberg.Scan<?, ?, ?> buildIcebergScanWithStats(Schema projection) {
    if (startSnapshotId != null) {
      return buildIcebergIncrementalAppendScan(projection, true /* with stats */);
    } else {
      return buildIcebergBatchScan(projection, false /* use residuals */, true /* with stats */);
    }
  }

  private IncrementalAppendScan buildIcebergIncrementalAppendScan(
      Schema projection, boolean withStats) {
    IncrementalAppendScan scan =
        table()
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(startSnapshotId)
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    scan = includeStats(scan, projection, withStats);

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    return configureSplitPlanning(scan);
  }

  private BatchScan buildIcebergBatchScan(
      Schema projection, boolean ignoreResiduals, boolean withStats) {
    if (shouldPinSnapshot() && snapshot == null) {
      return null;
    }

    BatchScan scan =
        newIcebergBatchScan()
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (shouldPinSnapshot() || timeTravel != null) {
      scan = scan.useSnapshot(snapshot.snapshotId());
    }

    Preconditions.checkState(
        Objects.equals(snapshot, scan.snapshot()),
        "Failed to enforce scan consistency: resolved Spark table snapshot (%s) vs scan snapshot"
            + " (%s)",
        snapshot,
        scan.snapshot());

    if (ignoreResiduals) {
      scan = scan.ignoreResiduals();
    }

    scan = includeStats(scan, projection, withStats);

    return configureSplitPlanning(scan);
  }

  private <S extends org.apache.iceberg.Scan<S, ?, ?>> S includeStats(
      S scan, Schema projection, boolean withStats) {
    if (withStats) {
      return scan.includeColumnStats();
    }
    List<String> variantColumns = variantColumnNames(projection);
    return variantColumns.isEmpty() ? scan : scan.includeColumnStats(variantColumns);
  }

  private List<String> variantColumnNames(Schema projection) {
    return projection.columns().stream()
        .filter(field -> field.type().isVariantType())
        .map(Types.NestedField::name)
        .collect(Collectors.toList());
  }

  private BatchScan newIcebergBatchScan() {
    if (readConf().distributedPlanningEnabled()) {
      return new SparkDistributedDataScan(spark(), table(), readConf());
    } else {
      return table().newBatchScan();
    }
  }

  private boolean shouldPinSnapshot() {
    return isMainTable() || isMetadataTableWithTimeTravel();
  }

  private boolean isMainTable() {
    return table() instanceof BaseTable;
  }

  private boolean isMetadataTableWithTimeTravel() {
    if (table() instanceof BaseMetadataTable metadataTable) {
      return metadataTable.supportsTimeTravel();
    } else {
      return false;
    }
  }
}
