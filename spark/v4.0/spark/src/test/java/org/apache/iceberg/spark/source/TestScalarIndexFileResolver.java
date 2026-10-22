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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.ScalarIndexFileResolver.TransformValueRange;
import org.junit.jupiter.api.Test;

class TestScalarIndexFileResolver {

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
}
