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

import static org.apache.iceberg.spark.source.ScalarIndexTestHelper.buildIndex;
import static org.apache.iceberg.spark.source.ScalarIndexTestHelper.row;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.ScalarIndexFileResolver.Resolution;
import org.apache.iceberg.spark.source.ScalarIndexFileResolver.TransformValueRange;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestScalarIndexFileResolver {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  @TempDir private File dir;

  private Table table;
  private DataFile fileA;
  private DataFile fileB;
  private DataFile fileC;

  @BeforeEach
  void createTable() {
    this.table =
        new HadoopTables(new Configuration())
            .create(SCHEMA, PartitionSpec.unpartitioned(), new File(dir, "table").toString());
    this.fileA = dataFile("a.parquet");
    this.fileB = dataFile("b.parquet");
    this.fileC = dataFile("c.parquet");
    table.newFastAppend().appendFile(fileA).appendFile(fileB).appendFile(fileC).commit();
  }

  private DataFile dataFile(String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(new File(dir, name).toString())
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private void buildDataIndex(String transform) {
    buildIndex(
        table,
        transform,
        ImmutableList.of(SCHEMA.findField("data")),
        ImmutableList.of(
            row(fileA.location(), "aaa"),
            row(fileB.location(), "bbb"),
            row(fileC.location(), "ccc")));
  }

  private void buildCompositeIndex() {
    buildIndex(
        table,
        "ZORDER",
        ImmutableList.of(SCHEMA.findField("id"), SCHEMA.findField("data")),
        ImmutableList.of(
            row(fileA.location(), 1L, "aaa"),
            row(fileB.location(), 2L, "bbb"),
            row(fileC.location(), 3L, "ccc")));
  }

  private Resolution resolve(Expression... filters) {
    return new ScalarIndexFileResolver(table, table.schema(), true, ImmutableList.copyOf(filters))
        .resolve(table.currentSnapshot());
  }

  @Test
  void presentKeyResolvesToItsFile() {
    buildDataIndex("HASH");

    Resolution resolution = resolve(Expressions.equal("data", "bbb"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).containsExactly(fileB.location());
    assertThat(resolution.uncoveredFilePaths()).isEmpty();
  }

  @Test
  void absentEqualityKeyResolvesToNoFiles() {
    buildDataIndex("HASH");

    Resolution resolution = resolve(Expressions.equal("data", "does-not-exist"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
    assertThat(resolution.uncoveredFilePaths()).isEmpty();
  }

  @Test
  void absentInKeysResolveToNoFiles() {
    buildDataIndex("HASH");

    Resolution resolution = resolve(Expressions.in("data", "missing-1", "missing-2"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
    assertThat(resolution.uncoveredFilePaths()).isEmpty();
  }

  @Test
  void absentIdentityRangeResolvesToNoFiles() {
    buildIndex(
        table,
        "IDENTITY",
        ImmutableList.of(SCHEMA.findField("id")),
        ImmutableList.of(row(fileA.location(), 1L), row(fileB.location(), 5L)));

    Resolution resolution = resolve(Expressions.greaterThan("id", 100L));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
    assertThat(resolution.uncoveredFilePaths()).isEmpty();
  }

  @Test
  void absentCompositeKeyResolvesToNoFiles() {
    buildCompositeIndex();

    Resolution resolution =
        resolve(Expressions.equal("id", 2L), Expressions.equal("data", "does-not-exist"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
    assertThat(resolution.uncoveredFilePaths()).isEmpty();
  }

  @Test
  void absentCompositeInKeysResolveToNoFiles() {
    buildCompositeIndex();

    Resolution resolution =
        resolve(Expressions.in("id", 1L, 3L), Expressions.in("data", "missing-1", "missing-2"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
  }

  @Test
  void presentCompositeKeyResolvesToItsFile() {
    buildCompositeIndex();

    Resolution resolution = resolve(Expressions.equal("id", 2L), Expressions.equal("data", "bbb"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).containsExactly(fileB.location());
  }

  @Test
  void absentKeyKeepsFilesAppendedAfterTheIndex() {
    buildDataIndex("HASH");
    DataFile appended = dataFile("d.parquet");
    table.newFastAppend().appendFile(appended).commit();

    Resolution resolution = resolve(Expressions.equal("data", "does-not-exist"));

    assertThat(resolution).isNotNull();
    assertThat(resolution.matchedFilePaths()).isEmpty();
    assertThat(resolution.uncoveredFilePaths()).containsExactly(appended.location());
  }

  @Test
  void nonAppendSnapshotSinceTheIndexFallsBack() {
    buildDataIndex("HASH");
    table.newDelete().deleteFile(fileA).commit();

    assertThat(resolve(Expressions.equal("data", "does-not-exist"))).isNull();
  }

  @Test
  void readSnapshotBeforeTheIndexFallsBack() {
    long beforeIndex = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(dataFile("d.parquet")).commit();
    buildDataIndex("HASH");

    Resolution resolution =
        new ScalarIndexFileResolver(
                table,
                table.schema(),
                true,
                ImmutableList.of(Expressions.equal("data", "does-not-exist")))
            .resolve(table.snapshot(beforeIndex));

    assertThat(resolution).isNull();
  }

  @Test
  void missingIndexFallsBack() {
    assertThat(resolve(Expressions.equal("data", "bbb"))).isNull();
  }

  @Test
  void literalOfAnotherTypeFallsBack() {
    buildIndex(
        table,
        "HASH",
        ImmutableList.of(SCHEMA.findField("id")),
        ImmutableList.of(row(fileA.location(), 1L), row(fileB.location(), 2L)));

    assertThat(resolve(Expressions.equal("id", "not-a-number"))).isNull();
    assertThat(resolve(Expressions.equal("id", 1.5D))).isNull();
  }

  @Test
  void rangeOnHashIndexFallsBack() {
    buildIndex(
        table,
        "HASH",
        ImmutableList.of(SCHEMA.findField("id")),
        ImmutableList.of(row(fileA.location(), 1L), row(fileB.location(), 2L)));

    assertThat(resolve(Expressions.greaterThan("id", 100L))).isNull();
  }

  @Test
  void exceedingTheCandidateLeafFileBoundFallsBack() {
    buildDataIndex("HASH");
    table
        .updateProperties()
        .set(ScalarIndexFileResolver.MAX_CANDIDATE_LEAF_FILES_PROPERTY, "0")
        .commit();

    assertThat(resolve(Expressions.equal("data", "bbb"))).isNull();
  }

  @Test
  void dedupesWhenTwoTargetRangesMatchTheSameLeafFileExactly() {
    // Two IN values that hash to the same bucket.
    List<TrackingFileEntry> candidates =
        ScalarIndexFileResolver.collectCandidateLeafFiles(
            ImmutableList.of(entry("leaf-a.parquet", 0, 0), entry("leaf-b.parquet", 1, 1)),
            ImmutableList.of(new TransformValueRange(0, 0), new TransformValueRange(0, 0)));

    assertThat(candidates)
        .extracting(TrackingFileEntry::location)
        .containsExactly("leaf-a.parquet");
  }

  @Test
  void dedupesWhenTwoTargetRangesOverlapTheSameWiderLeafFile() {
    List<TrackingFileEntry> candidates =
        ScalarIndexFileResolver.collectCandidateLeafFiles(
            ImmutableList.of(entry("leaf-wide.parquet", 0, 5)),
            ImmutableList.of(new TransformValueRange(1, 1), new TransformValueRange(4, 4)));

    assertThat(candidates)
        .extracting(TrackingFileEntry::location)
        .containsExactly("leaf-wide.parquet");
  }

  @Test
  void doesNotMergeDistinctLeafFilesFromDifferentRanges() {
    List<TrackingFileEntry> candidates =
        ScalarIndexFileResolver.collectCandidateLeafFiles(
            ImmutableList.of(entry("leaf-a.parquet", 0, 0), entry("leaf-b.parquet", 1, 1)),
            ImmutableList.of(new TransformValueRange(0, 0), new TransformValueRange(1, 1)));

    assertThat(candidates)
        .extracting(TrackingFileEntry::location)
        .containsExactlyInAnyOrder("leaf-a.parquet", "leaf-b.parquet");
  }

  @Test
  void returnsNoCandidatesWhenNoRangeOverlaps() {
    List<TrackingFileEntry> candidates =
        ScalarIndexFileResolver.collectCandidateLeafFiles(
            ImmutableList.of(entry("leaf-a.parquet", 0, 3)),
            ImmutableList.of(new TransformValueRange(4, 9)));

    assertThat(candidates).isEmpty();
  }

  private static TrackingFileEntry entry(String location, long lower, long upper) {
    return TrackingFileEntry.builder()
        .location(location)
        .fileFormat("parquet")
        .recordCount(1)
        .fileSizeInBytes(10)
        .transformValueLowerBound(lower)
        .transformValueUpperBound(upper)
        .build();
  }
}
