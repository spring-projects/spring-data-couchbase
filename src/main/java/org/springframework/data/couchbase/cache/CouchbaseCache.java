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
import org.springframework.cache.support.SimpleValueWrapper;

/**
 * The {@link CouchbaseCache} class implements the Spring Cache interface on top of Couchbase Server and the Couchbase
 * Java SDK.
 *
 * @see <a href="http://static.springsource.org/spring/docs/current/spring-framework-reference/html/cache.html">
 *   Official Spring Cache Reference</a>
 * @author Michael Nitschinger
 */
public class CouchbaseCache implements Cache {

  /**
   * The actual CouchbaseClient instance.
   */
  private final CouchbaseClient client;

  /**
   * The name of the cache.
   */
  private final String name;

  /**
   * Construct the cache and pass in the CouchbaseClient instance.
   *
   * @param name the name of the cache reference.
   * @param client the CouchbaseClient instance.
   */
  public CouchbaseCache(final String name, final CouchbaseClient client) {
    this.name = name;
    this.client = client;
  }

  /**
   * Returns the name of the cache.
   *
   * @return the name of the cache.
   */
  public final String getName() {
    return name;
  }

  /**
   * Returns the actual CouchbaseClient instance.
   *
   * @return the actual CouchbaseClient instance.
   */
  public final CouchbaseClient getNativeCache() {
    return client;
  }

  /**
   * Get an element from the cache.
   *
   * @param key the key to lookup against.
   * @return the fetched value from Couchbase.
   */
  public final ValueWrapper get(final Object key) {
    String documentId = key.toString();
    Object result = client.get(documentId);
    return (result != null ? new SimpleValueWrapper(result) : null);
  }

  /**
   * Store a object in Couchbase.
   *
   * @param key the Key of the storable object.
   * @param value the Object to store.
   */
  public final void put(final Object key, final Object value) {
    if (value != null) {
      String documentId = key.toString();
      client.set(documentId, 0, value);
    } else {
      evict(key);
    }
  }

  /**
   * Remove an object from Couchbase.
   *
   * @param key the Key of the object to delete.
   */
  public final void evict(final Object key) {
    String documentId = key.toString();
    client.delete(documentId);
  }

  /**
   * Clear the complete cache.
   *
   * Note that this action is very destructive, so only use it with care.
   * Also note that "flush" may not be enabled on the bucket.
   */
  public final void clear() {
    client.flush();
  }

}
