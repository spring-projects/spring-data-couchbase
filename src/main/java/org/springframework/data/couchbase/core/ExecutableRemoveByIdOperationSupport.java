/*
 * Copyright 2012-2021 the original author or authors
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

import java.util.Collection;
import java.util.List;

import org.springframework.data.couchbase.core.ReactiveRemoveByIdOperationSupport.ReactiveRemoveByIdSupport;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;

public class ExecutableRemoveByIdOperationSupport implements ExecutableRemoveByIdOperation {

	private final CouchbaseTemplate template;

	public ExecutableRemoveByIdOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public ExecutableRemoveById removeById() {
		return removeById(null);
	}

	@Override
	public ExecutableRemoveById removeById(Class<?> domainType) {
		return new ExecutableRemoveByIdSupport(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null);
	}

	static class ExecutableRemoveByIdSupport implements ExecutableRemoveById {

		private final CouchbaseTemplate template;
		private final Class<?> domainType;
		private final String scope;
		private final String collection;
		private final RemoveOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Long cas;
		private final ReactiveRemoveByIdSupport reactiveRemoveByIdSupport;

		ExecutableRemoveByIdSupport(final CouchbaseTemplate template, final Class<?> domainType, final String scope,
				final String collection, final RemoveOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, Long cas) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.reactiveRemoveByIdSupport = new ReactiveRemoveByIdSupport(template.reactive(), domainType, scope, collection,
					options, persistTo, replicateTo, durabilityLevel, cas);
			this.cas = cas;
		}

		@Override
		public RemoveResult one(final String id) {
			return reactiveRemoveByIdSupport.one(id).block();
		}

		@Override
		public List<RemoveResult> all(final Collection<String> ids) {
			return reactiveRemoveByIdSupport.all(ids).collectList().block();
		}

		@Override
		public RemoveByIdWithOptions inCollection(final String collection) {
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInCollection withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInCollection withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public TerminatingRemoveById withOptions(final RemoveOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdInCollection inScope(final String scope) {
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}

		@Override
		public RemoveByIdWithDurability withCas(Long cas) {
			return new ExecutableRemoveByIdSupport(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, cas);
		}
	}

}
