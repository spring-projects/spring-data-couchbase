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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class CouchbaseCacheManager extends AbstractTransactionSupportingCacheManager {

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
			final Map<String, CouchbaseCacheConfiguration> initialCacheConfiguration,
			final boolean allowInFlightCacheCreation) {

		Assert.notNull(cacheWriter, "CacheWriter must not be null!");
		Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null!");

		this.cacheWriter = cacheWriter;
		this.defaultCacheConfig = defaultCacheConfiguration;
		this.initialCacheConfiguration = initialCacheConfiguration;
		this.allowInFlightCacheCreation = allowInFlightCacheCreation;
	}

	/**
	 * Create a new {@link CouchbaseCacheManager} with defaults applied.
	 *
	 * @param clientFactory must not be {@literal null}.
	 * @return new instance of {@link CouchbaseCacheManager}.
	 */
	public static CouchbaseCacheManager create(CouchbaseClientFactory clientFactory) {
		Assert.notNull(clientFactory, "ConnectionFactory must not be null!");
		return new CouchbaseCacheManager(new DefaultCouchbaseCacheWriter(clientFactory),
				CouchbaseCacheConfiguration.defaultCacheConfig(), new LinkedHashMap<>(), true);
	}

	/**
	 * Entry point for builder style {@link CouchbaseCacheManager} configuration.
	 *
	 * @param clientFactory must not be {@literal null}.
	 * @return new {@link CouchbaseCacheManagerBuilder}.
	 */
	public static CouchbaseCacheManagerBuilder builder(CouchbaseClientFactory clientFactory) {
		Assert.notNull(clientFactory, "ConnectionFactory must not be null!");
		return CouchbaseCacheManagerBuilder.fromConnectionFactory(clientFactory);
	}

	/**
	 * Entry point for builder style {@link CouchbaseCacheManager} configuration.
	 *
	 * @param cacheWriter must not be {@literal null}.
	 * @return new {@link CouchbaseCacheManagerBuilder}.
	 */
	public static CouchbaseCacheManagerBuilder builder(CouchbaseCacheWriter cacheWriter) {
		Assert.notNull(cacheWriter, "CacheWriter must not be null!");
		return CouchbaseCacheManagerBuilder.fromCacheWriter(cacheWriter);
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

	public static class CouchbaseCacheManagerBuilder {

		private final CouchbaseCacheWriter cacheWriter;
		private final Map<String, CouchbaseCacheConfiguration> initialCaches = new LinkedHashMap<>();
		boolean allowInFlightCacheCreation = true;
		private CouchbaseCacheConfiguration defaultCacheConfiguration = CouchbaseCacheConfiguration.defaultCacheConfig();
		private boolean enableTransactions;

		private CouchbaseCacheManagerBuilder(CouchbaseCacheWriter cacheWriter) {
			this.cacheWriter = cacheWriter;
		}

		/**
		 * Entry point for builder style {@link CouchbaseCacheManager} configuration.
		 *
		 * @param clientFactory must not be {@literal null}.
		 * @return new {@link CouchbaseCacheManagerBuilder}.
		 */
		public static CouchbaseCacheManagerBuilder fromConnectionFactory(CouchbaseClientFactory clientFactory) {
			Assert.notNull(clientFactory, "ConnectionFactory must not be null!");
			return builder(new DefaultCouchbaseCacheWriter(clientFactory));
		}

		/**
		 * Entry point for builder style {@link CouchbaseCacheManager} configuration.
		 *
		 * @param cacheWriter must not be {@literal null}.
		 * @return new {@link CouchbaseCacheManagerBuilder}.
		 */
		public static CouchbaseCacheManagerBuilder fromCacheWriter(CouchbaseCacheWriter cacheWriter) {
			Assert.notNull(cacheWriter, "CacheWriter must not be null!");
			return new CouchbaseCacheManagerBuilder(cacheWriter);
		}

		/**
		 * Define a default {@link CouchbaseCacheConfiguration} applied to dynamically created {@link CouchbaseCache}s.
		 *
		 * @param defaultCacheConfiguration must not be {@literal null}.
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder cacheDefaults(CouchbaseCacheConfiguration defaultCacheConfiguration) {
			Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null!");
			this.defaultCacheConfiguration = defaultCacheConfiguration;
			return this;
		}

		/**
		 * Enable {@link CouchbaseCache}s to synchronize cache put/evict operations with ongoing Spring-managed
		 * transactions.
		 *
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder transactionAware() {
			this.enableTransactions = true;
			return this;
		}

		/**
		 * Append a {@link Set} of cache names to be pre initialized with current {@link CouchbaseCacheConfiguration}.
		 * <strong>NOTE:</strong> This calls depends on {@link #cacheDefaults(CouchbaseCacheConfiguration)} using whatever
		 * default {@link CouchbaseCacheConfiguration} is present at the time of invoking this method.
		 *
		 * @param cacheNames must not be {@literal null}.
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder initialCacheNames(Set<String> cacheNames) {
			Assert.notNull(cacheNames, "CacheNames must not be null!");
			cacheNames.forEach(it -> withCacheConfiguration(it, defaultCacheConfiguration));
			return this;
		}

		/**
		 * Append a {@link Map} of cache name/{@link CouchbaseCacheConfiguration} pairs to be pre initialized.
		 *
		 * @param cacheConfigurations must not be {@literal null}.
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder withInitialCacheConfigurations(
				Map<String, CouchbaseCacheConfiguration> cacheConfigurations) {

			Assert.notNull(cacheConfigurations, "CacheConfigurations must not be null!");
			cacheConfigurations.forEach((cacheName, configuration) -> Assert.notNull(configuration,
					String.format("CouchbaseCacheConfiguration for cache %s must not be null!", cacheName)));

			this.initialCaches.putAll(cacheConfigurations);
			return this;
		}

		/**
		 * @param cacheName
		 * @param cacheConfiguration
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder withCacheConfiguration(String cacheName,
				CouchbaseCacheConfiguration cacheConfiguration) {

			Assert.notNull(cacheName, "CacheName must not be null!");
			Assert.notNull(cacheConfiguration, "CacheConfiguration must not be null!");

			this.initialCaches.put(cacheName, cacheConfiguration);
			return this;
		}

		/**
		 * Disable in-flight {@link org.springframework.cache.Cache} creation for unconfigured caches.
		 * <p>
		 * {@link CouchbaseCacheManager#getMissingCache(String)} returns {@literal null} for any unconfigured
		 * {@link org.springframework.cache.Cache} instead of a new {@link CouchbaseCache} instance. This allows eg.
		 * {@link org.springframework.cache.support.CompositeCacheManager} to chime in.
		 *
		 * @return this {@link CouchbaseCacheManagerBuilder}.
		 */
		public CouchbaseCacheManagerBuilder disableCreateOnMissingCache() {

			this.allowInFlightCacheCreation = false;
			return this;
		}

		/**
		 * Get the {@link Set} of cache names for which the builder holds {@link CouchbaseCacheConfiguration configuration}.
		 *
		 * @return an unmodifiable {@link Set} holding the name of caches for which a {@link CouchbaseCacheConfiguration
		 *         configuration} has been set.
		 */
		public Set<String> getConfiguredCaches() {
			return Collections.unmodifiableSet(this.initialCaches.keySet());
		}

		/**
		 * Get the {@link CouchbaseCacheConfiguration} for a given cache by its name.
		 *
		 * @param cacheName must not be {@literal null}.
		 * @return {@link Optional#empty()} if no {@link CouchbaseCacheConfiguration} set for the given cache name.
		 */
		public Optional<CouchbaseCacheConfiguration> getCacheConfigurationFor(String cacheName) {
			return Optional.ofNullable(this.initialCaches.get(cacheName));
		}

		/**
		 * Create new instance of {@link CouchbaseCacheManager} with configuration options applied.
		 *
		 * @return new instance of {@link CouchbaseCacheManager}.
		 */
		public CouchbaseCacheManager build() {
			CouchbaseCacheManager cm = new CouchbaseCacheManager(cacheWriter, defaultCacheConfiguration, initialCaches,
					allowInFlightCacheCreation);
			cm.setTransactionAware(enableTransactions);
			return cm;
		}

	}

}
