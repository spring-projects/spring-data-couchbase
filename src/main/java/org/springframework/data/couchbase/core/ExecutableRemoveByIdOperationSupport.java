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

import java.util.Collection;
import java.util.List;

import org.springframework.data.couchbase.core.ReactiveRemoveByIdOperationSupport.ReactiveRemoveByIdSupport;
import org.springframework.util.Assert;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public class ExecutableRemoveByIdOperationSupport implements ExecutableRemoveByIdOperation {

	private final CouchbaseTemplate template;

	public ExecutableRemoveByIdOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public ExecutableRemoveById removeById() {
		return new ExecutableRemoveByIdSupport(template, null, PersistTo.NONE, ReplicateTo.NONE, DurabilityLevel.NONE);
	}

	static class ExecutableRemoveByIdSupport implements ExecutableRemoveById {

		private final CouchbaseTemplate template;
		private final String collection;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final ReactiveRemoveByIdSupport reactiveRemoveByIdSupport;

		ExecutableRemoveByIdSupport(final CouchbaseTemplate template, final String collection, final PersistTo persistTo,
				final ReplicateTo replicateTo, final DurabilityLevel durabilityLevel) {
			this.template = template;
			this.collection = collection;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.reactiveRemoveByIdSupport = new ReactiveRemoveByIdSupport(template.reactive(), collection, persistTo,
					replicateTo, durabilityLevel);
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
		public TerminatingRemoveById inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ExecutableRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public RemoveByIdWithCollection withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ExecutableRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public RemoveByIdWithCollection withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ExecutableRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

	}

}
