/*
 * Copyright 2012-present the original author or authors
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

import java.time.Duration;

import org.jspecify.annotations.Nullable;

import com.couchbase.client.java.codec.Transcoder;

public interface CouchbaseCacheWriter {

	/**
	 * Write the given key/value pair to Couchbase an set the expiration time if defined.
	 *
	 * @param collectionName The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param expiry Optional expiration time. Can be {@literal null}.
	 * @param transcoder Optional transcoder to use. Can be {@literal null}.
	 */
	void put(String collectionName, String key, Object value, @Nullable Duration expiry, @Nullable Transcoder transcoder);

	/**
	 * Write the given value to Couchbase if the key does not already exist.
	 *
	 * @param collectionName The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param expiry Optional expiration time. Can be {@literal null}.
	 * @param transcoder Optional transcoder to use. Can be {@literal null}.
	 */
	@Nullable
	Object putIfAbsent(String collectionName, String key, Object value, @Nullable Duration expiry,
			@Nullable Transcoder transcoder);

	/**
	 * Write the given value to Couchbase if the key does not already exist.
	 *
	 * @param collectionName The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param expiry Optional expiration time. Can be {@literal null}.
	 * @param transcoder Optional transcoder to use. Can be {@literal null}.
	 * @param clazz Optional class for contentAs(clazz)
	 */
	@Nullable
	Object putIfAbsent(String collectionName, String key, Object value, @Nullable Duration expiry,
			@Nullable Transcoder transcoder, @Nullable Class<?> clazz);

	/**
	 * Get the binary value representation from Couchbase stored for the given key.
	 *
	 * @param collectionName must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param transcoder Optional transcoder to use. Can be {@literal null}.
	 * @return {@literal null} if key does not exist.
	 */
	@Nullable
	Object get(String collectionName, String key, @Nullable Transcoder transcoder);

	/**
	 * Get the binary value representation from Couchbase stored for the given key.
	 *
	 * @param collectionName must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param transcoder Optional transcoder to use. Can be {@literal null}.
	 * @param clazz Optional class for contentAs(clazz)
	 * @return {@literal null} if key does not exist.
	 */
	@Nullable
	Object get(String collectionName, String key, @Nullable Transcoder transcoder, @Nullable Class<?> clazz);

	/**
	 * Remove the given key from Couchbase.
	 *
	 * @param collectionName The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @return true if the document existed on removal, false otherwise.
	 */
	boolean remove(String collectionName, String key);

	/**
	 * Clears the cache with the given key pattern prefix.
	 *
	 * @param pattern the pattern to clear.
	 * @return the number of cleared items.
	 */
	long clear(String collectionName, String pattern);

}
