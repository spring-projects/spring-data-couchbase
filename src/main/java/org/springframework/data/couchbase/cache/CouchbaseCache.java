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

package org.springframework.data.couchbase.cache;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

/**
 * The CouchbaseCache class implements the Spring Cache interface
 * on top of Couchbase Server and the Couchbase Java SDK.
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
    return this.client;
  }

  /**
   * Get an element from the cache.
   *
   * @param key the key to lookup against.
   * @return the fetched value from Couchbase.
   */
  public final ValueWrapper get(final Object key) {
    String documentId = key.toString();
    Object result = this.client.get(documentId);
    return (result != null ? new SimpleValueWrapper(result) : null);
  }

  /**
   * Store a object in Couchbase.
   *
   * @param key the Key of the storable object.
   * @param value the Object to store.
   */
  public final void put(final Object key, final Object value) {
    String documentId = key.toString();
    this.client.set(documentId, 0, value);
  }

  /**
   * Remove an object from Couchbase.
   *
   * @param key the Key of the object to delete.
   */
  public final void evict(final Object key) {
    String documentId = key.toString();
    this.client.delete(documentId);
  }

  /**
   * Clear the complete cache.
   *
   * Note that this action is very destructive, so only use it with care.
   * Also note that "flush" may not be enabled on the bucket.
   */
  public final void clear() {
    this.client.flush();
  }

}
