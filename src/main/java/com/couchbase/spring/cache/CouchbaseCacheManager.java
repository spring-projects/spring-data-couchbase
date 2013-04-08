/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.cache;

import com.couchbase.client.CouchbaseClient;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

/**
 * The CouchbaseCacheManager orchestrates CouchbaseCache instances.
 * 
 * Since more than one current CouchbaseClient connection can be used
 * for caching, the CouchbaseCacheManager orchestrates and handles
 * them for the Spring Cache abstraction layer.
 */
public class CouchbaseCacheManager extends AbstractCacheManager {

  /**
   * Holds the reference to all stored CouchbaseClient cache connections.
   */
  private final HashMap<String, CouchbaseClient> clients;

  /**
   * Construct a new CouchbaseCacheManager.
   *
   * @param clients one ore more CouchbaseClients to reference.
   */
  public CouchbaseCacheManager(final HashMap<String, CouchbaseClient> clients) {
    this.clients = clients;
  }

  /**
   * Returns a Map of all CouchbaseClients with name.
   *
   * @return the actual CouchbaseClient instances.
   */
  public final HashMap<String, CouchbaseClient> getClients() {
    return this.clients;
  }

  /**
   * Populates all caches.
   *
   * @return a collection of loaded caches.
   */
  @Override
  protected final Collection<? extends Cache> loadCaches() {
    Collection<Cache> caches = new LinkedHashSet<Cache>();

    for (Map.Entry<String, CouchbaseClient> cache : this.clients.entrySet()) {
      caches.add(new CouchbaseCache(cache.getKey(), cache.getValue()));
    }

    return caches;
  }

}
