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
import org.apache.iceberg.io.InputFile;
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
 * the data files that can contain matching rows.
 *
 * <p>An equality predicate resolves to a single transform value and an {@code IN} predicate to one
 * transform value per literal. Range predicates ({@code <}, {@code <=}, {@code >}, {@code >=}) are
 * only resolved against order-preserving transforms. A composite (multi-column) index is used when
 * every key column is constrained; its bounding transform-value range is derived from the corners
 * of the query box. Only the first index that resolves is used.
 *
 * <p>A successful resolution contains the covered files with at least one matching index entry and
 * the files added to the table after the index's source snapshot ("uncovered" files). Both sets can
 * be empty, which means no file can contain a matching row. Resolution returns {@code null}, which
 * means "plan normally", when there is no usable index, a predicate is ineligible, the candidate
 * leaf-file bound is exceeded, a snapshot between the index's source snapshot and the snapshot
 * being read is not an append, a query literal or the stored key type does not match the key
 * column's type, or anything fails.
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
   * Returns the data files that can contain rows matching the filters when read at {@code
   * readSnapshot}, or {@code null} if the scan must be planned normally.
   */
  Resolution resolve(Snapshot readSnapshot) {
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

  private Resolution resolveOrNull(Snapshot readSnapshot) {
    if (readSnapshot == null || filters == null || filters.isEmpty()) {
      return null;
    }

    Map<Integer, List<KeyPredicate>> predicatesByFieldId = keyPredicatesByFieldId();
    if (predicatesByFieldId.isEmpty()) {
      return null;
    }

    List<IndexMetadata> indexes = listIndexes();
    if (indexes == null || indexes.isEmpty()) {
      return null;
    }

    Resolution composite = resolveComposite(indexes, predicatesByFieldId, readSnapshot);
    if (composite != null) {
      return composite;
    }

    for (Map.Entry<Integer, List<KeyPredicate>> entry : predicatesByFieldId.entrySet()) {
      Resolution resolution =
          resolveSingleColumn(indexes, entry.getKey(), entry.getValue(), readSnapshot);
      if (resolution != null) {
        return resolution;
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

  private List<TrackingFileEntry> readTrackingFile(String trackingFileLocation) {
    return TrackingFileReader.readAll(table.io().newInputFile(trackingFileLocation));
  }

  private Type storedKeyType(TrackingFileEntry leaf, Types.NestedField keyField) {
    return LeafFileReader.storedKeyType(newLeafInputFile(leaf), keyField);
  }

  private InputFile newLeafInputFile(TrackingFileEntry leaf) {
    return table.io().newInputFile(leaf.location());
  }

  /**
   * Groups the EQ, IN, and range predicates on top-level columns by field ID, with literals
   * converted to the column type. A column with a literal that cannot be represented exactly in the
   * column type is left out, so it is never resolved through an index.
   */
  private Map<Integer, List<KeyPredicate>> keyPredicatesByFieldId() {
    Map<Integer, List<KeyPredicate>> predicatesByFieldId = Maps.newLinkedHashMap();
    Set<Integer> ineligibleFieldIds = Sets.newHashSet();
    for (Expression expr : filters) {
      if (!(expr instanceof UnboundPredicate) || !PRUNABLE_OPS.contains(expr.op())) {
        continue;
      }

      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      if (!(predicate.term() instanceof NamedReference)) {
        continue;
      }

      Types.NestedField field = findField(((NamedReference<?>) predicate.term()).name());
      if (field == null) {
        continue;
      }

      KeyPredicate keyPredicate = KeyPredicate.of(field, predicate);
      if (keyPredicate == null) {
        ineligibleFieldIds.add(field.fieldId());
      } else {
        predicatesByFieldId
            .computeIfAbsent(field.fieldId(), id -> Lists.newArrayList())
            .add(keyPredicate);
      }
    }

    ineligibleFieldIds.forEach(predicatesByFieldId::remove);
    return predicatesByFieldId;
  }

  private Types.NestedField findField(String name) {
    return caseSensitive ? schema.findField(name) : schema.caseInsensitiveFindField(name);
  }

  private Resolution resolveSingleColumn(
      List<IndexMetadata> indexes, int fieldId, List<KeyPredicate> predicates, Snapshot snapshot) {
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

      return resolveFilePaths(metadata, ImmutableList.of(keyField), resolved, snapshot);
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

  private Resolution resolveComposite(
      List<IndexMetadata> indexes,
      Map<Integer, List<KeyPredicate>> predicatesByFieldId,
      Snapshot readSnapshot) {
    for (IndexMetadata metadata : indexes) {
      if (SCALAR_INDEX_TYPE.equals(metadata.type()) && metadata.keyColumnIds().size() > 1) {
        Resolution resolution = resolveComposite(metadata, predicatesByFieldId, readSnapshot);
        if (resolution != null) {
          return resolution;
        }
      }
    }

    return null;
  }

  private Resolution resolveComposite(
      IndexMetadata metadata,
      Map<Integer, List<KeyPredicate>> predicatesByFieldId,
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
              box.label,
              box.residual);
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
      IndexMetadata metadata, Map<Integer, List<KeyPredicate>> predicatesByFieldId) {
    QueryBox box = new QueryBox();
    for (int fieldId : metadata.keyColumnIds()) {
      Types.NestedField keyField = schema.findField(fieldId);
      List<KeyPredicate> predicates = predicatesByFieldId.get(fieldId);
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
   * Reads the index's tracking file and candidate leaf files and returns the covered files with
   * matches plus any uncovered files, or {@code null} to plan normally.
   */
  private Resolution resolveFilePaths(
      IndexMetadata metadata,
      List<Types.NestedField> keyFields,
      ResolvedPredicate resolved,
      Snapshot readSnapshot) {
    IndexSnapshot indexSnapshot = metadata.currentSnapshot();
    if (indexSnapshot == null) {
      return null;
    }

    List<TrackingFileEntry> trackedLeafFiles = readTrackingFile(indexSnapshot.trackingFile());
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

    Set<String> matchedFilePaths = Sets.newHashSet();
    for (TrackingFileEntry leaf : candidateLeafFiles) {
      for (LeafFileEntry match :
          LeafFileReader.readMatching(newLeafInputFile(leaf), keyFields, resolved.leafFilter)) {
        matchedFilePaths.add(match.filePath());
      }
    }

    LOG.info(
        "SCALAR index on {} resolved {} to {} covered file(s) with matches and {} uncovered"
            + " file(s) after reading {} leaf file(s)",
        resolved.label,
        resolved.description,
        matchedFilePaths.size(),
        uncoveredFilePaths.size(),
        candidateLeafFiles.size());
    return new Resolution(matchedFilePaths, uncoveredFilePaths);
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
      Type storedKeyType = storedKeyType(leaf, keyField);
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

  private static ResolvedPredicate resolvePredicates(
      IndexMetadata metadata, Types.NestedField keyField, List<KeyPredicate> predicates) {
    TransformFunction transform =
        TransformFunctions.forName(metadata.transformFunction(), metadata.properties());
    for (KeyPredicate predicate : predicates) {
      if (predicate.op == Expression.Operation.EQ || predicate.op == Expression.Operation.IN) {
        return resolvePoints(transform, keyField, predicate);
      }
    }

    return resolveRange(transform, keyField, predicates);
  }

  private static ResolvedPredicate resolvePoints(
      TransformFunction transform, Types.NestedField keyField, KeyPredicate predicate) {
    if (predicate.values.isEmpty()) {
      return null;
    }

    List<TransformValueRange> ranges = Lists.newArrayListWithExpectedSize(predicate.values.size());
    for (Object value : predicate.values) {
      long transformValue = transform.transform(ImmutableList.of(value));
      ranges.add(new TransformValueRange(transformValue, transformValue));
    }

    return new ResolvedPredicate(
        ranges, predicate.toExpression(keyField.name()), keyField.name(), predicate.values);
  }

  private static ResolvedPredicate resolveRange(
      TransformFunction transform, Types.NestedField keyField, List<KeyPredicate> predicates) {
    // Only order-preserving transforms map a range on the column to a contiguous range of
    // transform values; HASH scatters values across buckets.
    if (!transform.supportsRangePredicates()) {
      return null;
    }

    long lowerBound = Long.MIN_VALUE;
    long upperBound = Long.MAX_VALUE;
    Expression leafFilter = Expressions.alwaysTrue();
    for (KeyPredicate predicate : predicates) {
      long value = ((Number) predicate.value()).longValue();
      if (predicate.isLowerBound()) {
        // Inclusive even for GT: the tracking-file range only needs to be a superset, the exact
        // predicate is re-applied to the leaf entries.
        lowerBound = Math.max(lowerBound, value);
      } else {
        upperBound = Math.min(upperBound, value);
      }

      leafFilter = Expressions.and(leafFilter, predicate.toExpression(keyField.name()));
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
  private static ColumnBound columnBound(List<KeyPredicate> predicates) {
    for (KeyPredicate predicate : predicates) {
      if (predicate.op == Expression.Operation.EQ || predicate.op == Expression.Operation.IN) {
        return pointsBound(predicate.values);
      }
    }

    Object lower = null;
    Object upper = null;
    for (KeyPredicate predicate : predicates) {
      if (predicate.isLowerBound()) {
        lower = predicate.value();
      } else {
        upper = predicate.value();
      }
    }

    return lower == null || upper == null ? null : new ColumnBound(lower, upper);
  }

  private static ColumnBound pointsBound(List<Object> values) {
    Object min = null;
    Object max = null;
    for (Object value : values) {
      if (min == null || compareValues(value, min) < 0) {
        min = value;
      }

      if (max == null || compareValues(value, max) > 0) {
        max = value;
      }
    }

    return min == null ? null : new ColumnBound(min, max);
  }

  @SuppressWarnings("unchecked")
  private static int compareValues(Object left, Object right) {
    return ((Comparable<Object>) left).compareTo(right);
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

  /** The data files an index lookup resolved to. */
  static class Resolution {
    private final Set<String> matchedFilePaths;
    private final Set<String> uncoveredFilePaths;

    Resolution(Set<String> matchedFilePaths, Set<String> uncoveredFilePaths) {
      this.matchedFilePaths = ImmutableSet.copyOf(matchedFilePaths);
      this.uncoveredFilePaths = ImmutableSet.copyOf(uncoveredFilePaths);
    }

    /** Paths of the covered data files that contain at least one matching index entry. */
    Set<String> matchedFilePaths() {
      return matchedFilePaths;
    }

    /** Paths of the data files added after the index's source snapshot. */
    Set<String> uncoveredFilePaths() {
      return uncoveredFilePaths;
    }
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

  /** A pushed-down predicate on a key column with literals converted to the column type. */
  private static class KeyPredicate {
    private final Expression.Operation op;
    private final List<Object> values;

    private KeyPredicate(Expression.Operation op, List<Object> values) {
      this.op = op;
      this.values = values;
    }

    /** Returns null if a literal cannot be represented exactly in the field's type. */
    private static KeyPredicate of(Types.NestedField field, UnboundPredicate<?> predicate) {
      List<? extends Literal<?>> literals =
          predicate.op() == Expression.Operation.IN
              ? predicate.literals()
              : ImmutableList.of(predicate.literal());
      List<Object> values = Lists.newArrayListWithExpectedSize(literals.size());
      for (Literal<?> literal : literals) {
        Object value = keyValue(field.type(), literal);
        if (value == null) {
          return null;
        }

        values.add(value);
      }

      return new KeyPredicate(predicate.op(), values);
    }

    private static Object keyValue(Type type, Literal<?> literal) {
      if (!type.isPrimitiveType()) {
        return null;
      }

      Literal<?> converted = literal.to(type);
      if (converted == null) {
        return null;
      }

      Object value;
      try {
        value = converted.value();
      } catch (UnsupportedOperationException e) {
        // AboveMax or BelowMin: the literal is outside the range of the column type.
        return null;
      }

      if (value instanceof CharSequence) {
        value = value.toString();
      }

      return type.typeId().javaClass().isInstance(value) ? value : null;
    }

    private Object value() {
      return values.get(0);
    }

    private boolean isLowerBound() {
      return op == Expression.Operation.GT || op == Expression.Operation.GT_EQ;
    }

    private Expression toExpression(String name) {
      if (op == Expression.Operation.IN) {
        return Expressions.in(name, values);
      }

      return Expressions.predicate(op, name, values.get(0));
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

    private void add(Types.NestedField keyField, ColumnBound bound, List<KeyPredicate> predicates) {
      keyFields.add(keyField);
      lowerCorner.add(bound.lower);
      upperCorner.add(bound.upper);
      columnNames.add(keyField.name());
      for (KeyPredicate predicate : predicates) {
        this.residual = Expressions.and(residual, predicate.toExpression(keyField.name()));
      }

      this.label = String.join(",", columnNames);
    }
  }
}
