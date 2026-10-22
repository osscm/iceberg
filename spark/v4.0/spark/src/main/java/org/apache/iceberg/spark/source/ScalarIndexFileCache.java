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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import org.apache.iceberg.index.LeafFileReader;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileReader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Driver-side caches for the immutable files of SCALAR indexes: tracking-file contents, the key
 * types stored in leaf files, and leaf-file Parquet footers.
 *
 * <p>Entries are keyed by file location without expiry. This is only correct because these files
 * are never rewritten in place: tracking files are named with a sequence number and a random suffix
 * and leaf files with a random UUID, so a rebuild writes new locations.
 */
class ScalarIndexFileCache {

  private static final long MAX_TRACKING_ENTRIES = 1_000_000L;
  private static final long MAX_KEY_TYPES = 10_000L;
  private static final long MAX_FOOTER_BYTES = 256L * 1024 * 1024;

  private static final ScalarIndexFileCache SHARED = new ScalarIndexFileCache();

  private final Cache<String, List<TrackingFileEntry>> trackingFiles;
  private final Cache<String, Type> storedKeyTypes;
  private final Cache<String, byte[]> leafTails;

  ScalarIndexFileCache() {
    this.trackingFiles =
        Caffeine.newBuilder()
            .maximumWeight(MAX_TRACKING_ENTRIES)
            .weigher((String location, List<TrackingFileEntry> entries) -> entries.size() + 1)
            .build();
    this.storedKeyTypes = Caffeine.newBuilder().maximumSize(MAX_KEY_TYPES).build();
    this.leafTails =
        Caffeine.newBuilder()
            .maximumWeight(MAX_FOOTER_BYTES)
            .weigher((String location, byte[] tail) -> tail.length + 1)
            .build();
  }

  static ScalarIndexFileCache shared() {
    return SHARED;
  }

  List<TrackingFileEntry> trackingEntries(FileIO io, String trackingFileLocation) {
    return trackingFiles.get(
        trackingFileLocation,
        location -> ImmutableList.copyOf(TrackingFileReader.readAll(io.newInputFile(location))));
  }

  /** The key type stored in {@code leaf}, or null if the leaf does not contain the key column. */
  Type storedKeyType(FileIO io, TrackingFileEntry leaf, Types.NestedField keyField) {
    String key = leaf.location() + "#" + keyField.fieldId();
    Type cached = storedKeyTypes.getIfPresent(key);
    if (cached != null) {
      return cached;
    }

    Type storedType = LeafFileReader.storedKeyType(leafInputFile(io, leaf), keyField);
    if (storedType != null) {
      storedKeyTypes.put(key, storedType);
    }

    return storedType;
  }

  /** Opens {@code leaf} with its known length and a cached Parquet footer. */
  InputFile leafInputFile(FileIO io, TrackingFileEntry leaf) {
    InputFile file =
        leaf.fileSizeInBytes() > 0
            ? io.newInputFile(leaf.location(), leaf.fileSizeInBytes())
            : io.newInputFile(leaf.location());
    return new FooterCachingInputFile(file, leafTails);
  }
}
