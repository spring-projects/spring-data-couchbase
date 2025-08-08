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
import java.util.Collection;

import org.springframework.data.couchbase.core.ReactiveUpsertByIdOperationSupport.ReactiveUpsertByIdSupport;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.UpsertOptions;

public class ExecutableUpsertByIdOperationSupport implements ExecutableUpsertByIdOperation {

	private final CouchbaseTemplate template;

	public ExecutableUpsertByIdOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableUpsertById<T> upsertById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ExecutableUpsertByIdSupport<>(template, domainType, OptionsBuilder.getScopeFrom(domainType),
				OptionsBuilder.getCollectionFrom(domainType), null, OptionsBuilder.getPersistTo(domainType),
				OptionsBuilder.getReplicateTo(domainType), OptionsBuilder.getDurabilityLevel(domainType, template.getConverter()),
				null);
	}

	static class ExecutableUpsertByIdSupport<T> implements ExecutableUpsertById<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final UpsertOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final ReactiveUpsertByIdSupport<T> reactiveSupport;

		ExecutableUpsertByIdSupport(final CouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final UpsertOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.reactiveSupport = new ReactiveUpsertByIdSupport<>(template.reactive(), domainType, scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry, new NonReactiveSupportWrapper(template.support()));
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
		public TerminatingUpsertById<T> withOptions(final UpsertOptions options) {
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope, collection,
					options != null ? options : this.options, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public UpsertByIdInCollection<T> inScope(final String scope) {
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope != null ? scope : this.scope, collection,
					options, persistTo, replicateTo, durabilityLevel, expiry);
		}

		@Override
		public UpsertByIdWithOptions<T> inCollection(final String collection) {
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope,
					collection != null ? collection : this.collection, options, persistTo, replicateTo, durabilityLevel, expiry);
		}

		@Override
		public UpsertByIdInScope<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public UpsertByIdInScope<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public UpsertByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ExecutableUpsertByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

	}

}
