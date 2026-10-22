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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Caches the SCALAR index listing of each table for a caller-supplied time-to-live.
 *
 * <p>A listing that fails is never cached. A listing that fails because the registry location does
 * not exist is treated as "no indexes": object stores list an absent prefix as empty, while HDFS
 * and local file systems throw.
 */
class IndexListingCache {

  private static final int DEFAULT_MAX_TABLES = 1_000;

  private final Cache<String, TimedListing> listings;
  private final LongSupplier nanoClock;

  IndexListingCache() {
    this(DEFAULT_MAX_TABLES, System::nanoTime);
  }

  IndexListingCache(int maxTables, LongSupplier nanoClock) {
    this.listings = Caffeine.newBuilder().maximumSize(maxTables).build();
    this.nanoClock = nanoClock;
  }

  /**
   * Returns the listing for {@code key}, loading it with {@code loader} unless a listing loaded
   * less than {@code ttlMs} ago is cached. A non-positive TTL always loads and caches nothing.
   */
  List<IndexMetadata> list(String key, long ttlMs, Supplier<List<IndexMetadata>> loader) {
    if (ttlMs <= 0) {
      return load(loader);
    }

    long now = nanoClock.getAsLong();
    TimedListing cached = listings.getIfPresent(key);
    if (cached != null && now - cached.loadedAtNanos < TimeUnit.MILLISECONDS.toNanos(ttlMs)) {
      return cached.indexes;
    }

    List<IndexMetadata> indexes = load(loader);
    listings.put(key, new TimedListing(indexes, now));
    return indexes;
  }

  void invalidate(String key) {
    listings.invalidate(key);
  }

  private static List<IndexMetadata> load(Supplier<List<IndexMetadata>> loader) {
    try {
      return ImmutableList.copyOf(loader.get());
    } catch (RuntimeException e) {
      if (isNotFound(e)) {
        return ImmutableList.of();
      }

      throw e;
    }
  }

  private static boolean isNotFound(Throwable throwable) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (cause instanceof FileNotFoundException
          || cause instanceof NoSuchFileException
          || cause instanceof NotFoundException) {
        return true;
      }

      if (cause.getCause() == cause) {
        break;
      }
    }

    return false;
  }

  private static class TimedListing {
    private final List<IndexMetadata> indexes;
    private final long loadedAtNanos;

    private TimedListing(List<IndexMetadata> indexes, long loadedAtNanos) {
      this.indexes = indexes;
      this.loadedAtNanos = loadedAtNanos;
    }
  }
}
