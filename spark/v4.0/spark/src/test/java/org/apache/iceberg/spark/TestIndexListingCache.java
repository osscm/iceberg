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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.index.DurableIndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIndexListingCache {

  private static final String KEY = "table-location#table-uuid";
  private static final long TTL_MS = 30_000L;

  @TempDir private File dir;

  private final AtomicLong nanos = new AtomicLong(0L);
  private final IndexListingCache cache = new IndexListingCache(10, nanos::get);
  private final AtomicInteger loads = new AtomicInteger();

  private Supplier<List<IndexMetadata>> countingLoader() {
    return () -> {
      loads.incrementAndGet();
      return ImmutableList.of();
    };
  }

  private void advanceMillis(long millis) {
    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
  }

  @Test
  void reusesListingWithinTtl() {
    cache.list(KEY, TTL_MS, countingLoader());
    advanceMillis(TTL_MS - 1);
    cache.list(KEY, TTL_MS, countingLoader());

    assertThat(loads).hasValue(1);
  }

  @Test
  void relistsAfterTtl() {
    cache.list(KEY, TTL_MS, countingLoader());
    advanceMillis(TTL_MS);
    cache.list(KEY, TTL_MS, countingLoader());

    assertThat(loads).hasValue(2);
  }

  @Test
  void zeroTtlRelistsEveryTime() {
    cache.list(KEY, 0L, countingLoader());
    cache.list(KEY, 0L, countingLoader());
    cache.list(KEY, 0L, countingLoader());

    assertThat(loads).hasValue(3);
  }

  @Test
  void invalidateForcesRelist() {
    cache.list(KEY, TTL_MS, countingLoader());
    cache.invalidate(KEY);
    cache.list(KEY, TTL_MS, countingLoader());

    assertThat(loads).hasValue(2);
  }

  @Test
  void failedListingIsNotCached() {
    assertThatThrownBy(
            () ->
                cache.list(
                    KEY,
                    TTL_MS,
                    () -> {
                      throw new IllegalStateException("Listing failed");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Listing failed");

    cache.list(KEY, TTL_MS, countingLoader());
    cache.list(KEY, TTL_MS, countingLoader());

    assertThat(loads).hasValue(1);
  }

  @Test
  void notFoundListingIsNoIndexes() {
    List<IndexMetadata> fromFileNotFound =
        cache.list(
            "a",
            TTL_MS,
            () -> {
              throw new UncheckedIOException(new FileNotFoundException("missing"));
            });
    List<IndexMetadata> fromNotFound =
        cache.list(
            "b",
            TTL_MS,
            () -> {
              throw new NotFoundException("missing");
            });

    assertThat(fromFileNotFound).isEmpty();
    assertThat(fromNotFound).isEmpty();
  }

  @Test
  void missingRegistryDirectoryIsCachedAsNoIndexes() {
    CountingFileIO io = new CountingFileIO();
    DurableIndexCatalog catalog =
        new DurableIndexCatalog(io, new File(dir, "missing-table").toURI().toString(), "uuid");
    Supplier<List<IndexMetadata>> loader =
        () -> catalog.listIndexes(TableIdentifier.of("db", "table"));

    assertThat(cache.list(KEY, TTL_MS, loader)).isEmpty();
    assertThat(cache.list(KEY, TTL_MS, loader)).isEmpty();
    assertThat(io.prefixListings()).isEqualTo(1);
  }
}
