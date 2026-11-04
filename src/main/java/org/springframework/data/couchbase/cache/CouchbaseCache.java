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


import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Couchbase-backed Cache Methods that take a Class return non-wrapped objects - cache-miss cannot be distinguished from
 * cached null - this is what AbstractValueAdaptingCache does Methods that do not take a Class return wrapped objects -
 * the wrapper is null for cache-miss - the exception is T get(final Object key, final Callable&lt;T&gt; valueLoader), which
 * does not return a wrapper because if there is a cache-miss, it gets the value from valueLoader (and caches it). There
 * are anomalies with get(key, ValueLoader) - which returns non-wrapped object.
 */
public class CouchbaseCache extends AbstractValueAdaptingCache {

	private final String name;
	private final CouchbaseCacheWriter cacheWriter;
	private final CouchbaseCacheConfiguration cacheConfig;
	private final ConversionService conversionService;

	protected CouchbaseCache(final String name, final CouchbaseCacheWriter cacheWriter,
			final CouchbaseCacheConfiguration cacheConfig) {
		super(cacheConfig.getAllowCacheNullValues());

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(cacheWriter, "CacheWriter must not be null!");
		Assert.notNull(cacheConfig, "CacheConfig must not be null!");

		this.name = name;
		this.cacheWriter = cacheWriter;
		this.cacheConfig = cacheConfig;
		this.conversionService = cacheConfig.getConversionService();
	}

	private static <T> T valueFromLoader(Object key, Callable<T> valueLoader) {
		try {
			return valueLoader.call();
		} catch (Exception e) {
			throw new ValueRetrievalException(key, valueLoader, e);
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public CouchbaseCacheWriter getNativeCache() {
		return cacheWriter;
	}

	/**
	 * same as inherited, but passes clazz for transcoder
	 */
	protected Object lookup(final Object key, Class<?> clazz) {
		return cacheWriter.get(cacheConfig.getCollectionName(), createCacheKey(key), cacheConfig.getValueTranscoder(),
				clazz);
	}

	@Override
	protected Object lookup(final Object key) {
		return lookup(key, Object.class);
	}
	/**
	 * Returns the configuration for this {@link CouchbaseCache}.
	 */
	public CouchbaseCacheConfiguration getCacheConfiguration() {
		return cacheConfig;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <T> T get(final Object key, final Callable<T> valueLoader) {
		ValueWrapper result = get(key);

		if (result != null) {
			return (T) result.get();
		}

		T value = valueFromLoader(key, valueLoader);
		put(key, value);
		return value;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(final Object key, Class<T> type) {
		Object value = this.fromStoreValue(this.lookup(key, type));
		if (value != null && type != null && !type.isInstance(value)) {
			throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
		} else {
			return (T) value;
		}
	}

	public synchronized <T> T get(final Object key, final Callable<T> valueLoader, Class<T> type) {
		T value = get(key, type);
		if (value == null) { // cannot distinguish between cache miss and cached null
			value = valueFromLoader(key, valueLoader);
			put(key, value);
		}
		return value;
	}

	@Override
	public void put(final Object key, final Object value) {
		cacheWriter.put(cacheConfig.getCollectionName(), createCacheKey(key), toStoreValue(value), cacheConfig.getExpiry(),
				cacheConfig.getValueTranscoder());
	}

	@Override
	public ValueWrapper putIfAbsent(final Object key, final Object value) {

		Object result = cacheWriter.putIfAbsent(cacheConfig.getCollectionName(), createCacheKey(key), toStoreValue(value),
				cacheConfig.getExpiry(), cacheConfig.getValueTranscoder());

		return toValueWrapper(result);
	}

	/**
	 * Not sure why this isn't in AbstractValueAdaptingCache
	 * 
	 * @param key
	 * @param value
	 * @param clazz
	 * @return
	 * @param <T>
	 */
	@SuppressWarnings("unchecked")
	public <T> T putIfAbsent(final Object key, final Object value, final Class<T> clazz) {

		Object result = cacheWriter.putIfAbsent(cacheConfig.getCollectionName(), createCacheKey(key),
				toStoreValue(value), cacheConfig.getExpiry(), cacheConfig.getValueTranscoder(), clazz);

		return (T) result;
	}

	@Override
	public void evict(final Object key) {
		cacheWriter.remove(cacheConfig.getCollectionName(), createCacheKey(key));
	}

	@Override
	public boolean evictIfPresent(final Object key) {
		return cacheWriter.remove(cacheConfig.getCollectionName(), createCacheKey(key));
	}

	@Override
	public boolean invalidate() {
		return cacheWriter.clear(cacheConfig.getCollectionName(), cacheConfig.getKeyPrefixFor(name)) > 0;
	}

	@Override
	public void clear() {
		cacheWriter.clear( cacheConfig.getCollectionName(), cacheConfig.getKeyPrefixFor(name));
	}

	/**
	 * Customization hook for creating cache key before it gets serialized.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected String createCacheKey(final Object key) {
		String convertedKey = convertKey(key);
		if (!cacheConfig.usePrefix()) {
			return convertedKey;
		}
		return prefixCacheKey(convertedKey);
	}

	/**
	 * Convert {@code key} to a {@link String} representation used for cache key creation.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalStateException if {@code key} cannot be converted to {@link String}.
	 */
	protected String convertKey(final Object key) {
		if (key == null) {
			throw new IllegalArgumentException(String.format("Cache '%s' does not allow 'null' key.", name));
		}
		if (key instanceof String) {
			return (String) key;
		}

		TypeDescriptor source = TypeDescriptor.forObject(key);

		if (conversionService.canConvert(source, TypeDescriptor.valueOf(String.class))) {
			try {
				return conversionService.convert(key, String.class);
			} catch (ConversionFailedException e) {
				// may fail if the given key is a collection
				if (isCollectionLikeOrMap(source)) {
					return convertCollectionLikeOrMapKey(key, source);
				}
				throw e;
			}
		}

		Method toString = ReflectionUtils.findMethod(key.getClass(), "toString");
		if (toString != null && !Object.class.equals(toString.getDeclaringClass())) {
			return key.toString();
		}

		throw new IllegalStateException(String.format(
				"Cannot convert cache key %s to String. Please register a suitable Converter via "
						+ "'CouchbaseCacheConfiguration.configureKeyConverters(...)' or override '%s.toString()'.",
				source, key.getClass().getSimpleName()));
	}

	private String prefixCacheKey(final String key) {
		// allow contextual cache names by computing the key prefix on every call.
		return cacheConfig.getKeyPrefixFor(name) + key;
	}

	private boolean isCollectionLikeOrMap(final TypeDescriptor source) {
		return source.isArray() || source.isCollection() || source.isMap();
	}

	private String convertCollectionLikeOrMapKey(final Object key, final TypeDescriptor source) {
		if (source.isMap()) {
			StringBuilder target = new StringBuilder("{");

			for (Map.Entry<?, ?> entry : ((Map<?, ?>) key).entrySet()) {
				target.append(convertKey(entry.getKey())).append("=").append(convertKey(entry.getValue()));
			}
			target.append("}");

			return target.toString();
		} else if (source.isCollection() || source.isArray()) {
			StringJoiner sj = new StringJoiner(",");

			Collection<?> collection = source.isCollection() ? (Collection<?>) key
					: Arrays.asList(ObjectUtils.toObjectArray(key));

			for (Object val : collection) {
				sj.add(convertKey(val));
			}
			return "[" + sj.toString() + "]";
		}

		throw new IllegalArgumentException(String.format("Cannot convert cache key %s to String.", key));
	}

}
