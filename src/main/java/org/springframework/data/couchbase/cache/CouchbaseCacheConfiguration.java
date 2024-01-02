/*
 * Copyright 2012-2024 the original author or authors
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.util.Assert;

import com.couchbase.client.java.codec.SerializableTranscoder;
import com.couchbase.client.java.codec.Transcoder;

public class CouchbaseCacheConfiguration {

	private final Duration expiry;
	private final boolean cacheNullValues;
	private final CacheKeyPrefix keyPrefix;
	private final boolean usePrefix;
	private final Transcoder valueTranscoder;
	private final ConversionService conversionService;
	private final String collectionName;

	private CouchbaseCacheConfiguration(final Duration expiry, final boolean cacheNullValues, final boolean usePrefix,
			final CacheKeyPrefix keyPrefix, final ConversionService conversionService, final Transcoder valueTranscoder,
			final String collectionName) {
		this.expiry = expiry;
		this.cacheNullValues = cacheNullValues;
		this.usePrefix = usePrefix;
		this.keyPrefix = keyPrefix;
		this.conversionService = conversionService;
		this.valueTranscoder = valueTranscoder;
		this.collectionName = collectionName;
	}

	public static CouchbaseCacheConfiguration defaultCacheConfig() {
		DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
		registerDefaultConverters(conversionService);

		return new CouchbaseCacheConfiguration(Duration.ZERO, true, true, CacheKeyPrefix.simple(), conversionService,
				SerializableTranscoder.INSTANCE, null);
	}

	/**
	 * Registers default cache key converters. The following converters get registered:
	 * <ul>
	 * <li>{@link String} to {@link byte byte[]} using UTF-8 encoding.</li>
	 * <li>{@link SimpleKey} to {@link String}</li>
	 * </ul>
	 * 
	 * @param registry must not be {@literal null}.
	 */
	public static void registerDefaultConverters(final ConverterRegistry registry) {
		Assert.notNull(registry, "ConverterRegistry must not be null!");
		registry.addConverter(String.class, byte[].class, source -> source.getBytes(StandardCharsets.UTF_8));
		registry.addConverter(SimpleKey.class, String.class, SimpleKey::toString);
	}

	/**
	 * Set the expiry to apply for cache entries. Use {@link Duration#ZERO} to declare an eternal cache.
	 *
	 * @param expiry must not be {@literal null}.
	 * @return new {@link CouchbaseCacheConfiguration}.
	 */
	public CouchbaseCacheConfiguration entryExpiry(final Duration expiry) {
		Assert.notNull(expiry, "Expiry duration must not be null!");
		return new CouchbaseCacheConfiguration(expiry, cacheNullValues, usePrefix, keyPrefix, conversionService,
				valueTranscoder, collectionName);
	}

	/**
	 * Set the collectinName to use.
	 *
	 * @param collectionName must not be {@literal null}.
	 * @return new {@link CouchbaseCacheConfiguration}.
	 */
	public CouchbaseCacheConfiguration collection(final String collectionName) {
		Assert.notNull(collectionName, "collectionName must not be null!");
		return new CouchbaseCacheConfiguration(expiry, cacheNullValues, usePrefix, keyPrefix, conversionService,
				valueTranscoder, collectionName);
	}

	/**
	 * Sets a custom transcoder to use for reads and writes.
	 *
	 * @param valueTranscoder the transcoder that should be used.
	 * @return new {@link CouchbaseCacheConfiguration}.
	 */
	public CouchbaseCacheConfiguration valueTranscoder(final Transcoder valueTranscoder) {
		Assert.notNull(valueTranscoder, "Transcoder must not be null!");
		return new CouchbaseCacheConfiguration(expiry, cacheNullValues, usePrefix, keyPrefix, conversionService,
				valueTranscoder, collectionName);
	}

	/**
	 * Disable caching {@literal null} values. <br />
	 * <strong>NOTE</strong> any {@link org.springframework.cache.Cache#put(Object, Object)} operation involving
	 * {@literal null} value will error. Nothing will be written to Couchbase, nothing will be removed. An already
	 * existing key will still be there afterwards with the very same value as before.
	 *
	 * @return new {@link CouchbaseCacheConfiguration}.
	 */
	public CouchbaseCacheConfiguration disableCachingNullValues() {
		return new CouchbaseCacheConfiguration(expiry, false, usePrefix, keyPrefix, conversionService, valueTranscoder,
				collectionName);
	}

	/**
	 * Prefix the {@link CouchbaseCache#getName() cache name} with the given value. <br />
	 * The generated cache key will be: {@code prefix + cache name + "::" + cache entry key}.
	 *
	 * @param prefix the prefix to prepend to the cache name.
	 * @return this.
	 * @see #computePrefixWith(CacheKeyPrefix)
	 * @see CacheKeyPrefix#prefixed(String)
	 */
	public CouchbaseCacheConfiguration prefixCacheNameWith(final String prefix) {
		return computePrefixWith(CacheKeyPrefix.prefixed(prefix));
	}

	/**
	 * Use the given {@link CacheKeyPrefix} to compute the prefix for the actual Couchbase {@literal key} given the
	 * {@literal cache name} as function input.
	 *
	 * @param cacheKeyPrefix must not be {@literal null}.
	 * @return new {@link CouchbaseCacheConfiguration}.
	 * @see CacheKeyPrefix
	 */
	public CouchbaseCacheConfiguration computePrefixWith(CacheKeyPrefix cacheKeyPrefix) {
		Assert.notNull(cacheKeyPrefix, "Function for computing prefix must not be null!");
		return new CouchbaseCacheConfiguration(expiry, cacheNullValues, true, cacheKeyPrefix, conversionService,
				valueTranscoder, collectionName);
	}

	/**
	 * @return The expiration time (ttl) for cache entries. Never {@literal null}.
	 */
	public Duration getExpiry() {
		return expiry;
	}

	/**
	 * @return {@literal true} if caching {@literal null} is allowed.
	 */
	public boolean getAllowCacheNullValues() {
		return cacheNullValues;
	}

	/**
	 * @return The {@link ConversionService} used for cache key to {@link String} conversion. Never {@literal null}.
	 */
	public ConversionService getConversionService() {
		return conversionService;
	}

	/**
	 * @return {@literal true} if cache keys need to be prefixed with the {@link #getKeyPrefixFor(String)} if present or
	 *         the default which resolves to {@link Cache#getName()}.
	 */
	public boolean usePrefix() {
		return usePrefix;
	}

	/**
	 * Get the computed {@literal key} prefix for a given {@literal cacheName}.
	 *
	 * @return never {@literal null}.
	 */
	public String getKeyPrefixFor(final String cacheName) {
		Assert.notNull(cacheName, "Cache name must not be null!");
		return keyPrefix.compute(cacheName);
	}

	/**
	 * Get the transcoder for encoding and decoding cache values.
	 */
	public Transcoder getValueTranscoder() {
		return valueTranscoder;
	}

	/**
	 * The name of the collection to use for this cache - if empty uses the default collection.
	 */
	public String getCollectionName() {
		return collectionName;
	}

}
