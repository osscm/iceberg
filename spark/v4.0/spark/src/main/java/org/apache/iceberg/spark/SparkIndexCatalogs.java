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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.index.DurableIndexCatalog;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Process-wide registry of {@link IndexCatalog} instances, one per table, shared between {@code
 * CALL system.build_scalar_index(...)} and query-time index lookups. Follows the same
 * singleton-registry pattern as {@link ScanTaskSetManager}.
 *
 * <p>Backed by {@link DurableIndexCatalog}, so index registrations survive a JVM restart. Index
 * listings are cached per table for a caller-supplied time-to-live and invalidated when this
 * process builds an index; registrations made by other processes become visible once the cached
 * listing expires.
 */
public class SparkIndexCatalogs {

  private static final SparkIndexCatalogs INSTANCE = new SparkIndexCatalogs();

  private final Map<String, IndexCatalog> catalogsByTableUuid = Maps.newConcurrentMap();
  private final IndexListingCache listings = new IndexListingCache();

  private SparkIndexCatalogs() {}

  public static SparkIndexCatalogs get() {
    return INSTANCE;
  }

  /** The {@link IndexCatalog} for {@code table}, created on first use. */
  public IndexCatalog catalogFor(Table table) {
    return catalogsByTableUuid.computeIfAbsent(
        Spark3Util.baseTableUUID(table),
        uuid -> new DurableIndexCatalog(table.io(), table.location(), uuid));
  }

  /**
   * Lists the indexes of {@code table}, reusing a listing loaded less than {@code cacheTtlMs} ago.
   * A non-positive TTL always lists. A missing index registry lists as no indexes.
   */
  public List<IndexMetadata> listIndexes(Table table, long cacheTtlMs) {
    // Derived from the core table name, exactly as BuildScalarIndexProcedure derives it.
    TableIdentifier tableIdentifier = TableIdentifier.parse(table.name());
    return listings.list(
        listingKey(table), cacheTtlMs, () -> catalogFor(table).listIndexes(tableIdentifier));
  }

  /** Drops the cached index listing of {@code table}, for example after building an index. */
  public void invalidateIndexes(Table table) {
    listings.invalidate(listingKey(table));
  }

  private static String listingKey(Table table) {
    // The location alone is not unique: a dropped and recreated table can reuse it.
    return table.location() + "#" + Spark3Util.baseTableUUID(table);
  }
}
