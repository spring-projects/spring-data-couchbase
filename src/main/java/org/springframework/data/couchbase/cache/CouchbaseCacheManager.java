/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.cache;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CouchbaseCacheManager extends AbstractCacheManager {

  private final CouchbaseCacheWriter cacheWriter;
  private final CouchbaseCacheConfiguration defaultCacheConfig;
  private final Map<String, CouchbaseCacheConfiguration> initialCacheConfiguration;
  private final boolean allowInFlightCacheCreation;

  /**
   * Creates new {@link CouchbaseCacheManager} using given {@link CouchbaseCacheWriter} and default
   * {@link CouchbaseCacheConfiguration}.
   *
   * @param cacheWriter must not be {@literal null}.
   * @param defaultCacheConfiguration must not be {@literal null}. Maybe just use
   *          {@link CouchbaseCacheConfiguration#defaultCacheConfig()}.
   * @param allowInFlightCacheCreation allow create unconfigured caches.
   */
  private CouchbaseCacheManager(final CouchbaseCacheWriter cacheWriter,
                                final CouchbaseCacheConfiguration defaultCacheConfiguration,
                                final boolean allowInFlightCacheCreation) {

    Assert.notNull(cacheWriter, "CacheWriter must not be null!");
    Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null!");

    this.cacheWriter = cacheWriter;
    this.defaultCacheConfig = defaultCacheConfiguration;
    this.initialCacheConfiguration = new LinkedHashMap<>();
    this.allowInFlightCacheCreation = allowInFlightCacheCreation;
  }

  public static CouchbaseCacheManager create(final CouchbaseClientFactory connectionFactory) {
    return new CouchbaseCacheManager(
      new DefaultCouchbaseCacheWriter(connectionFactory),
      CouchbaseCacheConfiguration.defaultCacheConfig(),
      true
    );
  }

  @Override
  protected Collection<? extends Cache> loadCaches() {
    final List<CouchbaseCache> caches = new LinkedList<>();

    for (Map.Entry<String, CouchbaseCacheConfiguration> entry : initialCacheConfiguration.entrySet()) {
      caches.add(createCouchbaseCache(entry.getKey(), entry.getValue()));
    }

    return caches;
  }

  @Override
  protected CouchbaseCache getMissingCache(final String name) {
    return allowInFlightCacheCreation ? createCouchbaseCache(name, defaultCacheConfig) : null;
  }

  /**
   * Configuration hook for creating {@link CouchbaseCache} with given name and {@code cacheConfig}.
   *
   * @param name must not be {@literal null}.
   * @param cacheConfig can be {@literal null}.
   * @return never {@literal null}.
   */
  protected CouchbaseCache createCouchbaseCache(final String name,
                                                @Nullable final CouchbaseCacheConfiguration cacheConfig) {
    return new CouchbaseCache(name, cacheWriter, cacheConfig != null ? cacheConfig : defaultCacheConfig);
  }

}
