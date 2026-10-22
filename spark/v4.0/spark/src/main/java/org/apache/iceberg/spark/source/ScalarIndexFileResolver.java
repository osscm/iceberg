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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileReader;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileReader;
import org.apache.iceberg.index.TransformFunction;
import org.apache.iceberg.index.TransformFunctions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.IndexSnapshotUtil;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the pushed-down filters of a batch scan against the table's SCALAR indexes, producing
 * the set of data file paths that can contain matching rows.
 *
 * <p>An equality predicate resolves to a single transform value and an {@code IN} predicate to one
 * transform value per literal. Range predicates ({@code <}, {@code <=}, {@code >}, {@code >=}) are
 * only resolved against order-preserving transforms. A composite (multi-column) index is used when
 * every key column is constrained; its bounding transform-value range is derived from the corners
 * of the query box. Only the first index that resolves is used.
 *
 * <p>Files added to the table after the index's source snapshot ("uncovered" files) are always
 * included. Any failure, an ineligible predicate, a non-append snapshot between the index's source
 * snapshot and the snapshot being read, or a key column whose type no longer matches the type
 * stored in the index resolves to {@code null}, which means "plan normally".
 */
class ScalarIndexFileResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ScalarIndexFileResolver.class);

  private static final String SCALAR_INDEX_TYPE = "SCALAR";

  // Bounds the number of leaf files a single lookup may open, so that a misconfigured index (for
  // example a very large hash.num-buckets, or a wide IN list) cannot make planning slower than
  // planning without the index.
  static final String MAX_CANDIDATE_LEAF_FILES_PROPERTY = "scalar-index.max-candidate-leaf-files";
  static final int DEFAULT_MAX_CANDIDATE_LEAF_FILES = 100;

  private static final Set<Expression.Operation> PRUNABLE_OPS =
      ImmutableSet.of(
          Expression.Operation.EQ,
          Expression.Operation.IN,
          Expression.Operation.LT,
          Expression.Operation.LT_EQ,
          Expression.Operation.GT,
          Expression.Operation.GT_EQ);

  private final Table table;
  private final Schema schema;
  private final boolean caseSensitive;
  private final List<Expression> filters;

  ScalarIndexFileResolver(
      Table table, Schema schema, boolean caseSensitive, List<Expression> filters) {
    this.table = table;
    this.schema = schema;
    this.caseSensitive = caseSensitive;
    this.filters = filters;
  }

  /**
   * Returns the paths of the data files that can contain rows matching the filters when read at
   * {@code readSnapshot}, or {@code null} if the scan must be planned normally.
   */
  Set<String> resolve(Snapshot readSnapshot) {
    try {
      return resolveOrNull(readSnapshot);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to resolve SCALAR indexes for table {}, falling back to normal planning: {}",
          table.name(),
          e.getMessage());
      return null;
    }
  }

  private Set<String> resolveOrNull(Snapshot readSnapshot) {
    if (readSnapshot == null || filters == null || filters.isEmpty()) {
      return null;
    }

    Map<Integer, List<UnboundPredicate<?>>> predicatesByFieldId = candidatePredicatesByFieldId();
    if (predicatesByFieldId.isEmpty()) {
      return null;
    }

    List<IndexMetadata> indexes = listIndexes();
    if (indexes == null || indexes.isEmpty()) {
      return null;
    }

    Set<String> compositePaths = resolveComposite(indexes, predicatesByFieldId, readSnapshot);
    if (compositePaths != null) {
      return compositePaths;
    }

    for (Map.Entry<Integer, List<UnboundPredicate<?>>> entry : predicatesByFieldId.entrySet()) {
      Set<String> paths =
          resolveSingleColumn(indexes, entry.getKey(), entry.getValue(), readSnapshot);
      if (paths != null) {
        return paths;
      }
    }

    return null;
  }

  private List<IndexMetadata> listIndexes() {
    // Derived from the core table name, exactly as BuildScalarIndexProcedure derives it, so both
    // sides address the same registry entries.
    TableIdentifier tableIdentifier = TableIdentifier.parse(table.name());
    try {
      return SparkIndexCatalogs.get().catalogFor(table).listIndexes(tableIdentifier);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to list SCALAR indexes for table {}, falling back to normal planning: {}",
          tableIdentifier,
          e.getMessage());
      return null;
    }
  }

  /** Groups the EQ, IN, and range predicates on top-level named columns by field ID. */
  private Map<Integer, List<UnboundPredicate<?>>> candidatePredicatesByFieldId() {
    Map<Integer, List<UnboundPredicate<?>>> predicatesByFieldId = Maps.newLinkedHashMap();
    for (Expression expr : filters) {
      if (!(expr instanceof UnboundPredicate) || !PRUNABLE_OPS.contains(expr.op())) {
        continue;
      }

      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      if (!(predicate.term() instanceof NamedReference)) {
        continue;
      }

      Types.NestedField field = findField(((NamedReference<?>) predicate.term()).name());
      if (field != null) {
        predicatesByFieldId
            .computeIfAbsent(field.fieldId(), id -> Lists.newArrayList())
            .add(predicate);
      }
    }

    return predicatesByFieldId;
  }

  private Types.NestedField findField(String name) {
    return caseSensitive ? schema.findField(name) : schema.caseInsensitiveFindField(name);
  }

  private Set<String> resolveSingleColumn(
      List<IndexMetadata> indexes,
      int fieldId,
      List<UnboundPredicate<?>> predicates,
      Snapshot readSnapshot) {
    IndexMetadata metadata = findSingleColumnIndex(indexes, fieldId);
    if (metadata == null) {
      return null;
    }

    Types.NestedField keyField = schema.findField(fieldId);
    try {
      ResolvedPredicate resolved = resolvePredicates(metadata, keyField, predicates);
      if (resolved == null) {
        return null;
      }

      return resolveFilePaths(metadata, ImmutableList.of(keyField), resolved, readSnapshot);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to use SCALAR index on column {}, falling back to normal planning: {}",
          keyField.name(),
          e.getMessage());
      return null;
    }
  }

  private static IndexMetadata findSingleColumnIndex(List<IndexMetadata> indexes, int fieldId) {
    for (IndexMetadata metadata : indexes) {
      if (SCALAR_INDEX_TYPE.equals(metadata.type())
          && metadata.keyColumnIds().size() == 1
          && metadata.keyColumnIds().get(0) == fieldId) {
        return metadata;
      }
    }

    return null;
  }

  private Set<String> resolveComposite(
      List<IndexMetadata> indexes,
      Map<Integer, List<UnboundPredicate<?>>> predicatesByFieldId,
      Snapshot readSnapshot) {
    for (IndexMetadata metadata : indexes) {
      if (SCALAR_INDEX_TYPE.equals(metadata.type()) && metadata.keyColumnIds().size() > 1) {
        Set<String> paths = resolveComposite(metadata, predicatesByFieldId, readSnapshot);
        if (paths != null) {
          return paths;
        }
      }
    }

    return null;
  }

  private Set<String> resolveComposite(
      IndexMetadata metadata,
      Map<Integer, List<UnboundPredicate<?>>> predicatesByFieldId,
      Snapshot readSnapshot) {
    QueryBox box = queryBox(metadata, predicatesByFieldId);
    if (box == null) {
      return null;
    }

    try {
      TransformFunction transform =
          TransformFunctions.forName(metadata.transformFunction(), metadata.properties());
      long cornerA = transform.transform(box.lowerCorner);
      long cornerB = transform.transform(box.upperCorner);
      ResolvedPredicate resolved =
          new ResolvedPredicate(
              ImmutableList.of(
                  new TransformValueRange(Math.min(cornerA, cornerB), Math.max(cornerA, cornerB))),
              box.residual,
              box.label);
      return resolveFilePaths(metadata, box.keyFields, resolved, readSnapshot);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to use composite SCALAR index on {}, falling back to normal planning: {}",
          box.label,
          e.getMessage());
      return null;
    }
  }

  /**
   * Builds the bounded query box for a composite index, or returns {@code null} if any key column
   * is not bounded on both sides by the filters.
   */
  private QueryBox queryBox(
      IndexMetadata metadata, Map<Integer, List<UnboundPredicate<?>>> predicatesByFieldId) {
    QueryBox box = new QueryBox();
    for (int fieldId : metadata.keyColumnIds()) {
      Types.NestedField keyField = schema.findField(fieldId);
      List<UnboundPredicate<?>> predicates = predicatesByFieldId.get(fieldId);
      if (keyField == null || predicates == null) {
        return null;
      }

      ColumnBound bound = columnBound(predicates);
      if (bound == null) {
        return null;
      }

      box.add(keyField, bound, predicates);
    }

    return box;
  }

  /**
   * Reads the index's tracking file and candidate leaf files and returns the covered matching files
   * plus any uncovered files, or {@code null} to plan normally.
   */
  private Set<String> resolveFilePaths(
      IndexMetadata metadata,
      List<Types.NestedField> keyFields,
      ResolvedPredicate resolved,
      Snapshot readSnapshot) {
    IndexSnapshot indexSnapshot = metadata.currentSnapshot();
    if (indexSnapshot == null) {
      return null;
    }

    FileIO io = table.io();
    List<TrackingFileEntry> trackedLeafFiles =
        TrackingFileReader.readAll(io.newInputFile(indexSnapshot.trackingFile()));
    if (keyTypeChanged(trackedLeafFiles, keyFields, resolved.label)) {
      return null;
    }

    // Throws if the index's snapshot is not an append-only ancestor of the snapshot being read;
    // callers treat that as "plan normally".
    Set<String> uncoveredFilePaths = uncoveredFilePaths(indexSnapshot, readSnapshot);

    List<TrackingFileEntry> candidateLeafFiles =
        collectCandidateLeafFiles(trackedLeafFiles, resolved.targetRanges);
    if (exceedsCandidateLeafFileBound(candidateLeafFiles.size(), resolved.label)) {
      return null;
    }

    List<LeafFileEntry> matches = Lists.newArrayList();
    for (TrackingFileEntry leaf : candidateLeafFiles) {
      matches.addAll(
          LeafFileReader.readMatching(
              io.newInputFile(leaf.location()), keyFields, resolved.leafFilter));
    }

    if (matches.isEmpty() && uncoveredFilePaths.isEmpty()) {
      // Pruning to zero files is deliberately not attempted; the residual predicate still filters
      // every row of a normal scan.
      return null;
    }

    Set<String> resolvedPaths = Sets.newHashSet(uncoveredFilePaths);
    matches.forEach(match -> resolvedPaths.add(match.filePath()));
    LOG.info(
        "SCALAR index on {} resolved {} to {} file(s) ({} covered match(es), {} uncovered file(s))",
        resolved.label,
        resolved.description,
        resolvedPaths.size(),
        matches.size(),
        uncoveredFilePaths.size());
    return resolvedPaths;
  }

  private Set<String> uncoveredFilePaths(IndexSnapshot indexSnapshot, Snapshot readSnapshot) {
    if (indexSnapshot.sourceTableSnapshotId() == readSnapshot.snapshotId()) {
      return ImmutableSet.of();
    }

    return IndexSnapshotUtil.addedFilePathsSince(
        table, indexSnapshot.sourceTableSnapshotId(), readSnapshot.snapshotId());
  }

  private boolean exceedsCandidateLeafFileBound(int candidateLeafFileCount, String label) {
    int maxCandidateLeafFiles =
        PropertyUtil.propertyAsInt(
            table.properties(),
            MAX_CANDIDATE_LEAF_FILES_PROPERTY,
            DEFAULT_MAX_CANDIDATE_LEAF_FILES);
    if (candidateLeafFileCount > maxCandidateLeafFiles) {
      LOG.info(
          "SCALAR index on {} resolved to {} candidate leaf file(s), exceeding the planning-cost"
              + " bound of {} ({}), falling back to normal planning",
          label,
          candidateLeafFileCount,
          maxCandidateLeafFiles,
          MAX_CANDIDATE_LEAF_FILES_PROPERTY);
      return true;
    }

    return false;
  }

  /**
   * Returns true if a key column's current type differs from the type stored in the index (for
   * example after an int to long promotion), in which case transform values computed from query
   * literals no longer match the stored ones.
   */
  private boolean keyTypeChanged(
      List<TrackingFileEntry> trackedLeafFiles, List<Types.NestedField> keyFields, String label) {
    if (trackedLeafFiles.isEmpty()) {
      return false;
    }

    TrackingFileEntry leaf = trackedLeafFiles.get(0);
    for (Types.NestedField keyField : keyFields) {
      Type storedKeyType =
          LeafFileReader.storedKeyType(table.io().newInputFile(leaf.location()), keyField);
      if (!keyField.type().equals(storedKeyType)) {
        LOG.warn(
            "SCALAR index on {} was built for key column {} of type {} but the current schema has"
                + " {}, falling back to normal planning; rebuild the index to re-enable pruning",
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
      IndexMetadata metadata, Types.NestedField keyField, List<UnboundPredicate<?>> predicates) {
    TransformFunction transform =
        TransformFunctions.forName(metadata.transformFunction(), metadata.properties());
    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.EQ) {
        return resolveEq(transform, keyField, predicate.literal().value());
      }
    }

    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.IN) {
        return resolveIn(transform, keyField, predicate.literals());
      }
    }

    return resolveRange(transform, keyField, predicates);
  }

  private static ResolvedPredicate resolveEq(
      TransformFunction transform, Types.NestedField keyField, Object value) {
    long transformValue = transform.transform(ImmutableList.of(value));
    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(transformValue, transformValue)),
        Expressions.equal(keyField.name(), value),
        keyField.name(),
        value);
  }

  private static ResolvedPredicate resolveIn(
      TransformFunction transform,
      Types.NestedField keyField,
      List<? extends Literal<?>> literals) {
    if (literals.isEmpty()) {
      return null;
    }

    List<Object> values = Lists.newArrayListWithExpectedSize(literals.size());
    List<TransformValueRange> ranges = Lists.newArrayListWithExpectedSize(literals.size());
    for (Literal<?> literal : literals) {
      Object value = literal.value();
      long transformValue = transform.transform(ImmutableList.of(value));
      values.add(value);
      ranges.add(new TransformValueRange(transformValue, transformValue));
    }

    return new ResolvedPredicate(
        ranges, Expressions.in(keyField.name(), values), keyField.name(), values);
  }

  private static ResolvedPredicate resolveRange(
      TransformFunction transform,
      Types.NestedField keyField,
      List<UnboundPredicate<?>> predicates) {
    // Only order-preserving transforms map a range on the column to a contiguous range of
    // transform values; HASH scatters values across buckets.
    if (!transform.supportsRangePredicates()) {
      return null;
    }

    long lowerBound = Long.MIN_VALUE;
    long upperBound = Long.MAX_VALUE;
    Expression leafFilter = Expressions.alwaysTrue();
    for (UnboundPredicate<?> predicate : predicates) {
      long value = ((Number) predicate.literal().value()).longValue();
      if (predicate.op() == Expression.Operation.GT
          || predicate.op() == Expression.Operation.GT_EQ) {
        // Inclusive even for GT: the tracking-file range only needs to be a superset, the exact
        // predicate is re-applied to the leaf entries.
        lowerBound = Math.max(lowerBound, value);
      } else {
        upperBound = Math.min(upperBound, value);
      }

      leafFilter =
          Expressions.and(
              leafFilter,
              Expressions.predicate(predicate.op(), keyField.name(), predicate.literal()));
    }

    if (lowerBound > upperBound) {
      return null;
    }

    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(lowerBound, upperBound)),
        leafFilter,
        keyField.name(),
        "[" + lowerBound + ", " + upperBound + "]");
  }

  /**
   * Derives a bounded {@code [lower, upper]} for one key column of a composite query box, or {@code
   * null} if the column is not bounded on both sides.
   */
  private static ColumnBound columnBound(List<UnboundPredicate<?>> predicates) {
    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.EQ) {
        Object value = predicate.literal().value();
        return new ColumnBound(value, value);
      }
    }

    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.IN) {
        return inBound(predicate.literals());
      }
    }

    return rangeBound(predicates);
  }

  private static ColumnBound inBound(List<? extends Literal<?>> literals) {
    Object min = null;
    Object max = null;
    for (Literal<?> literal : literals) {
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

  private static ColumnBound rangeBound(List<UnboundPredicate<?>> predicates) {
    Object lower = null;
    Object upper = null;
    for (UnboundPredicate<?> predicate : predicates) {
      if (predicate.op() == Expression.Operation.GT
          || predicate.op() == Expression.Operation.GT_EQ) {
        lower = predicate.literal().value();
      } else if (predicate.op() == Expression.Operation.LT
          || predicate.op() == Expression.Operation.LT_EQ) {
        upper = predicate.literal().value();
      }
    }

    return lower == null || upper == null ? null : new ColumnBound(lower, upper);
  }

  @SuppressWarnings("unchecked")
  private static int compareValues(Object left, Object right) {
    return ((Comparable<Object>) left).compareTo(right);
  }

  /** Returns a copy of {@code predicate} that references {@code name}. */
  private static Expression rename(UnboundPredicate<?> predicate, String name) {
    if (predicate.op() == Expression.Operation.IN) {
      List<Object> values = Lists.newArrayList();
      predicate.literals().forEach(literal -> values.add(literal.value()));
      return Expressions.in(name, values);
    }

    return Expressions.predicate(predicate.op(), name, predicate.literal());
  }

  /**
   * Returns the tracking-file entries whose transform-value range overlaps any of {@code
   * targetRanges}, deduplicated by location so a leaf file is read at most once.
   */
  static List<TrackingFileEntry> collectCandidateLeafFiles(
      List<TrackingFileEntry> trackedLeafFiles, List<TransformValueRange> targetRanges) {
    Map<String, TrackingFileEntry> byLocation = Maps.newLinkedHashMap();
    for (TransformValueRange range : targetRanges) {
      for (TrackingFileEntry entry : trackedLeafFiles) {
        if (entry.transformValueLowerBound() <= range.max()
            && entry.transformValueUpperBound() >= range.min()) {
          byLocation.putIfAbsent(entry.location(), entry);
        }
      }
    }

    return Lists.newArrayList(byLocation.values());
  }

  /** An inclusive range of transform values to look up in the tracking file. */
  static class TransformValueRange {
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

  /** Target transform-value ranges and the exact leaf-entry filter resolved from predicates. */
  private static class ResolvedPredicate {
    private final List<TransformValueRange> targetRanges;
    private final Expression leafFilter;
    private final String label;
    private final Object description;

    private ResolvedPredicate(
        List<TransformValueRange> targetRanges,
        Expression leafFilter,
        String label,
        Object description) {
      this.targetRanges = targetRanges;
      this.leafFilter = leafFilter;
      this.label = label;
      this.description = description;
    }

    private ResolvedPredicate(
        List<TransformValueRange> targetRanges, Expression leafFilter, String label) {
      this(targetRanges, leafFilter, label, leafFilter);
    }
  }

  /** The bounded [lower, upper] value range for one key column of a composite query box. */
  private static class ColumnBound {
    private final Object lower;
    private final Object upper;

    private ColumnBound(Object lower, Object upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  /** The corners and exact residual filter of a composite index lookup. */
  private static class QueryBox {
    private final List<Types.NestedField> keyFields = Lists.newArrayList();
    private final List<Object> lowerCorner = Lists.newArrayList();
    private final List<Object> upperCorner = Lists.newArrayList();
    private final List<String> columnNames = Lists.newArrayList();
    private Expression residual = Expressions.alwaysTrue();
    private String label = "";

    private void add(
        Types.NestedField keyField, ColumnBound bound, List<UnboundPredicate<?>> predicates) {
      keyFields.add(keyField);
      lowerCorner.add(bound.lower);
      upperCorner.add(bound.upper);
      columnNames.add(keyField.name());
      for (UnboundPredicate<?> predicate : predicates) {
        this.residual = Expressions.and(residual, rename(predicate, keyField.name()));
      }

      this.label = String.join(",", columnNames);
    }
  }
}
