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

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.LegacyDocument;

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
  private final Bucket bucket;

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
  public CouchbaseCache(final String name, final Bucket client) {
    this.name = name;
    this.bucket = client;
  }

  /**
   * Returns the name of the cache.
   *
   * @return the name of the cache.
   */
  @Override
  public final String getName() {
    return name;
  }

  /**
   * Returns the actual CouchbaseClient instance.
   *
   * @return the actual CouchbaseClient instance.
   */
  @Override
  public final Bucket getNativeCache() {
    return bucket;
  }

  /**
   * Get an element from the cache.
   *
   * @param key the key to lookup against.
   * @return the fetched value from Couchbase.
   */
  @Override
  public final ValueWrapper get(final Object key) {
    LegacyDocument documentId = LegacyDocument.create(key.toString());
    LegacyDocument result = bucket.get(documentId);
    return (result != null ? new SimpleValueWrapper(result.content()) : null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public final <T> T get(final Object key, final Class<T> clazz) {
    LegacyDocument documentId = LegacyDocument.create(key.toString());
    LegacyDocument result = bucket.get(documentId);
    return (T) bucket.get(result).content();
  }

  /**
   * Store a object in Couchbase.
   *
   * @param key the Key of the storable object.
   * @param value the Object to store.
   */
  @Override
  public final void put(final Object key, final Object value) {
    if (value != null) {
      LegacyDocument document = LegacyDocument.create(key.toString(), value);
      bucket.upsert((Document<?>) document);
    } else {
      evict(key);
    }
  }

  /**
   * Remove an object from Couchbase.
   *
   * @param key the Key of the object to delete.
   */
  @Override
  public final void evict(final Object key) {
    LegacyDocument documentId = LegacyDocument.create(key.toString());
    bucket.remove(documentId);
  }

  /**
   * Clear the complete cache.
   *
   * Note that this action is very destructive, so only use it with care.
   * Also note that "flush" may not be enabled on the bucket.
   */
  @Override
  public final void clear() {
    bucket.bucketManager().flush();
  }

  @Override
  public ValueWrapper putIfAbsent(Object key, Object value) {
    final ValueWrapper valueWrapper = get(key);
    if (valueWrapper == null) {
      put(key, value);
      return new SimpleValueWrapper(value);
    }
    return valueWrapper;
  }

}
