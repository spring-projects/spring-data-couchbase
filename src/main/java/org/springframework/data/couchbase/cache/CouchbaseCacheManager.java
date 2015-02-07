/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.cache;

import com.couchbase.client.CouchbaseClient;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * The {@link CouchbaseCacheManager} orchestrates {@link CouchbaseCache} instances.
 * 
 * Since more than one current {@link CouchbaseClient} connection can be used for caching, the
 * {@link CouchbaseCacheManager} orchestrates and handles them for the Spring Cache abstraction layer.
 *
 * @author Michael Nitschinger
 * @author Konrad Król
 */
public class CouchbaseCacheManager extends AbstractCacheManager {

  /**
   * Holds the reference to all stored CouchbaseClient cache connections.
   */
  private final HashMap<String, CouchbaseClient> clients;
  
  /**
   * Holds the TTL configuration for each cache.
   */
  private final HashMap<String, Integer> ttlConfiguration;

  /**
   * Construct a new CouchbaseCacheManager.
   *
   * @param clients one ore more CouchbaseClients to reference.
   */
  public CouchbaseCacheManager(final HashMap<String, CouchbaseClient> clients) {
    this.clients = clients;
    this.ttlConfiguration = new HashMap<String, Integer>();
  }
  
  /**
   * Construct a new CouchbaseCacheManager.
   *
   * @param clients one ore more CouchbaseClients to reference.
   * @param ttlConfiguration one or more TTL values (in seconds)
   */
  public CouchbaseCacheManager(final HashMap<String, CouchbaseClient> clients, final HashMap<String, Integer> ttlConfiguration) {
    this.clients = clients;
    this.ttlConfiguration = ttlConfiguration;
  }

  /**
   * Returns a Map of all CouchbaseClients with name.
   *
   * @return the actual CouchbaseClient instances.
   */
  public final HashMap<String, CouchbaseClient> getClients() {
    return clients;
  }

  /**
   * Populates all caches.
   *
   * @return a collection of loaded caches.
   */
  @Override
  protected final Collection<? extends Cache> loadCaches() {
    Collection<Cache> caches = new LinkedHashSet<Cache>();

    for (Map.Entry<String, CouchbaseClient> cache : clients.entrySet()) {
      caches.add(new CouchbaseCache(cache.getKey(), cache.getValue(), getTtl(cache.getKey())));
    }

    return caches;
  }
  
  /**
   * Returns TTL value for single cache
   * @param name cache name
   * @return either the cache TTL value or 0 as a default value
   */
  private int getTtl ( String name ) {
      Integer expirationTime = ttlConfiguration.get(name);
      return (expirationTime != null ? expirationTime : 0);
  }

}
