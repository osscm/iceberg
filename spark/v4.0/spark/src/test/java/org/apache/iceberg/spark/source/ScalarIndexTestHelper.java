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

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.index.IndexIdentifier;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileMetadata;
import org.apache.iceberg.index.LeafFileWriter;
import org.apache.iceberg.index.ScalarIndexCommitter;
import org.apache.iceberg.index.TransformFunction;
import org.apache.iceberg.index.TransformFunctions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.apache.iceberg.types.Types;

/** Builds small SCALAR indexes directly through the core writers, without a Spark session. */
class ScalarIndexTestHelper {

  private ScalarIndexTestHelper() {}

  /** One indexed row: its key values and the data file that contains it. */
  static class IndexedRow {
    private final List<Object> keyValues;
    private final String filePath;

    IndexedRow(List<Object> keyValues, String filePath) {
      this.keyValues = keyValues;
      this.filePath = filePath;
    }
  }

  static IndexedRow row(String filePath, Object... keyValues) {
    return new IndexedRow(ImmutableList.copyOf(keyValues), filePath);
  }

  /**
   * Writes one leaf file containing {@code rows} and commits it as the index's current snapshot,
   * sourced from the table's current snapshot.
   */
  static void buildIndex(
      Table table, String transformName, List<Types.NestedField> keyFields, List<IndexedRow> rows) {
    TransformFunction transform = TransformFunctions.forName(transformName, ImmutableMap.of());
    List<LeafFileEntry> entries =
        rows.stream()
            .map(
                row ->
                    LeafFileEntry.builder()
                        .keyValues(row.keyValues)
                        .transformValue(transform.transform(row.keyValues))
                        .filePath(row.filePath)
                        .position(0L)
                        .build())
            .sorted(
                Comparator.comparingLong(LeafFileEntry::transformValue)
                    .thenComparing(LeafFileEntry::keyValues, ScalarIndexTestHelper::compareKeys))
            .collect(Collectors.toList());

    String indexName =
        keyFields.stream().map(Types.NestedField::name).collect(Collectors.joining("_")) + "_idx";
    String indexLocation = table.location() + "/index/" + indexName;
    String leafPath = indexLocation + "/data/leaf-" + UUID.randomUUID() + ".parquet";
    try (LeafFileWriter writer =
        new LeafFileWriter(
            table.io().newOutputFile(leafPath),
            keyFields,
            LeafFileWriter.DEFAULT_ROW_GROUP_SIZE_BYTES)) {
      writer.addAll(entries);
    }

    LeafFileMetadata leaf =
        new LeafFileMetadata(
            leafPath,
            "parquet",
            entries.size(),
            table.io().newInputFile(leafPath).getLength(),
            entries.get(0).transformValue(),
            entries.get(entries.size() - 1).transformValue());
    new ScalarIndexCommitter(SparkIndexCatalogs.get().catalogFor(table), table.io())
        .commit(
            IndexIdentifier.of(TableIdentifier.parse(table.name()), indexName),
            table.uuid().toString(),
            table.currentSnapshot().snapshotId(),
            "SCALAR",
            transformName,
            keyFields.stream().map(Types.NestedField::fieldId).collect(Collectors.toList()),
            indexLocation,
            ImmutableList.of(leaf));
  }

  @SuppressWarnings("unchecked")
  private static int compareKeys(List<Object> left, List<Object> right) {
    for (int i = 0; i < left.size(); i++) {
      int cmp = ((Comparable<Object>) left.get(i)).compareTo(right.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }
}
