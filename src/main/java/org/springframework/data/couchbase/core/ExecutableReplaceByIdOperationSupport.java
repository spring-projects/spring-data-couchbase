/*
 * Copyright 2012-2020 the original author or authors
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

import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public class ExecutableReplaceByIdOperationSupport implements ExecutableReplaceByIdOperation {

	private final CouchbaseTemplate template;

	public ExecutableReplaceByIdOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableReplaceById<T> replaceById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ExecutableReplaceByIdSupport<>(template, domainType, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null);
	}

	static class ExecutableReplaceByIdSupport<T> implements ExecutableReplaceById<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final String collection;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final ReactiveReplaceByIdOperationSupport.ReactiveReplaceByIdSupport<T> reactiveSupport;

		ExecutableReplaceByIdSupport(final CouchbaseTemplate template, final Class<T> domainType, final String collection,
				final PersistTo persistTo, final ReplicateTo replicateTo, final DurabilityLevel durabilityLevel,
				final Duration expiry) {
			this.template = template;
			this.domainType = domainType;
			this.collection = collection;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.reactiveSupport = new ReactiveReplaceByIdOperationSupport.ReactiveReplaceByIdSupport<>(template.reactive(),
					domainType, collection, persistTo, replicateTo, durabilityLevel, expiry);
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
		public TerminatingReplaceById<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ExecutableReplaceByIdSupport<>(template, domainType, collection, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public ReplaceByIdWithCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ExecutableReplaceByIdSupport<>(template, domainType, collection, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public ReplaceByIdWithCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ExecutableReplaceByIdSupport<>(template, domainType, collection, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

		@Override
		public ReplaceByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ExecutableReplaceByIdSupport<>(template, domainType, collection, persistTo, replicateTo,
					durabilityLevel, expiry);
		}

	}

}
