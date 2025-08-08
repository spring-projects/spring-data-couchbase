/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

/**
 * {@link ExecutableMutateInByIdOperation} implementations for Couchbase.
 *
 * @author Tigran Babloyan
 */
public class ExecutableMutateInByIdOperationSupport implements ExecutableMutateInByIdOperation {

	private final CouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ExecutableMutateInByIdOperationSupport.class);

	public ExecutableMutateInByIdOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableMutateInById<T> mutateInById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ExecutableMutateInByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, OptionsBuilder.getPersistTo(domainType),
				OptionsBuilder.getReplicateTo(domainType), OptionsBuilder.getDurabilityLevel(domainType, template.getConverter()),
				null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
				Collections.emptyList(), false);
	}

	static class ExecutableMutateInByIdSupport<T> implements ExecutableMutateInById<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final MutateInOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final boolean provideCas;
		private final List<String> removePaths = new ArrayList<>();
		private final List<String> upsertPaths = new ArrayList<>();
		private final List<String> insertPaths = new ArrayList<>();
		private final List<String> replacePaths = new ArrayList<>();
		private final ReactiveMutateInByIdOperationSupport.ReactiveMutateInByIdSupport<T> reactiveSupport;


		ExecutableMutateInByIdSupport(final CouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final MutateInOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry, final List<String> removePaths, 
				final List<String> upsertPaths, final List<String> insertPaths, final List<String> replacePaths, final boolean provideCas) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.removePaths.addAll(removePaths);	
			this.upsertPaths.addAll(upsertPaths);
			this.insertPaths.addAll(insertPaths);
			this.replacePaths.addAll(replacePaths);
			this.provideCas = provideCas;
			this.reactiveSupport = new ReactiveMutateInByIdOperationSupport.ReactiveMutateInByIdSupport<T>(template.reactive(), domainType, scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry, new NonReactiveSupportWrapper(template.support()), removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public T one(final T object) {
			return reactiveSupport.one(object).block();
		}

		@Override
		public Collection<? extends T> all(Collection<? extends T> objects) {
			return reactiveSupport.all(objects).collectList().block();
		}

		@Override
		public TerminatingMutateInById<T> withOptions(final MutateInOptions options) {
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection,
					options != null ? options : this.options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> inCollection(final String collection) {
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, persistTo, replicateTo, durabilityLevel, expiry,
					removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInCollection<T> inScope(final String scope) {
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry, removePaths, upsertPaths, insertPaths,
					replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withRemovePaths(final String... removePaths) {
			Assert.notNull(removePaths, "removePaths path must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, Arrays.asList(removePaths), upsertPaths, insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withUpsertPaths(final String... upsertPaths) {
			Assert.notNull(upsertPaths, "upsertPaths path must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, Arrays.asList(upsertPaths), insertPaths, replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withInsertPaths(final String... insertPaths) {
			Assert.notNull(insertPaths, "insertPaths path must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, Arrays.asList(insertPaths), replacePaths, provideCas);
		}

		@Override
		public MutateInByIdWithDurability<T> withReplacePaths(final String... replacePaths) {
			Assert.notNull(replacePaths, "replacePaths path must not be null.");
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, Arrays.asList(replacePaths), provideCas);
		}

		@Override
		public MutateInByIdWithPaths<T> withCasProvided() {
			return new ExecutableMutateInByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, removePaths, upsertPaths, insertPaths, replacePaths, true);
		}
	}

}
