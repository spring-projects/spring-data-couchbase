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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import org.springframework.data.couchbase.core.query.Query;

import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import reactor.core.publisher.Mono;

public class ReactiveRemoveByQueryOperationSupport implements ReactiveRemoveByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveCouchbaseTemplate template;

	public ReactiveRemoveByQueryOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveRemoveByQuery<T> removeByQuery(Class<T> domainType) {
		return new ReactiveRemoveByQuerySupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED,
				null /*"default:`"+template.getCouchbaseClientFactory().getBucket().name()+"`" */, null,
				PersistTo.NONE, ReplicateTo.NONE, DurabilityLevel.NONE);
	}

	static class ReactiveRemoveByQuerySupport<T> implements ReactiveRemoveByQuery<T>, ReactiveRemoveByIdOperation.ReactiveRemoveById {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;

		ReactiveRemoveByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final Query query,
																 final QueryScanConsistency scanConsistency, String scope, String collection,
																 PersistTo persistTo, ReplicateTo replicateTo, DurabilityLevel durabilityLevel) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
		}

		@Override
		public Flux<RemoveResult> all() {
			return Flux.defer(() -> {
				String statement = assembleDeleteQuery();

				return template.getCouchbaseClientFactory().getCluster().reactive().query(statement, query.buildQueryOptions(scanConsistency,
						scope))
						.onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						}).flatMapMany(ReactiveQueryResult::rowsAsObject)
						.map(row -> new RemoveResult(row.getString(TemplateUtils.SELECT_ID), row.getLong(TemplateUtils.SELECT_CAS), Optional.empty()));
			});
		}

		@Override
		public Mono<RemoveResult> one(final String id) {
			return Mono.just(id).flatMap(docId -> template.getCollection(collection).reactive()
					.remove(id, buildRemoveOptions()).map(r -> RemoveResult.from(docId, r))).onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		private RemoveOptions buildRemoveOptions() {
			final RemoveOptions options = RemoveOptions.removeOptions();
			if (persistTo != PersistTo.NONE || replicateTo != ReplicateTo.NONE) {
				options.durability(persistTo, replicateTo);
			} else if (durabilityLevel != DurabilityLevel.NONE) {
				options.durability(durabilityLevel);
			}
			return options;
		}

		@Override
		public ReactiveRemoveByIdOperation.TerminatingRemoveById inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveRemoveByIdOperationSupport.ReactiveRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public Flux<RemoveResult> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		@Override
		public TerminatingRemoveByQuery<T> matching(final Query query) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency,
					scope, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public RemoveByQueryWithQuery<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency,
					scope, collection, persistTo, replicateTo, durabilityLevel);
		}


		@Override
		public ReactiveRemoveByIdOperation.RemoveByIdWithCollection withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveRemoveByIdOperationSupport.ReactiveRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public ReactiveRemoveByIdOperation.RemoveByIdWithCollection withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveRemoveByIdOperationSupport.ReactiveRemoveByIdSupport(template, collection, persistTo, replicateTo, durabilityLevel);
		}

		@Override
		public RemoveByQueryConsistentWith<T> inScope(final String scope) {
			return new ReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection, persistTo, replicateTo, durabilityLevel);
		}
		private String assembleDeleteQuery() {
			return query.toN1qlRemoveString(template, this.domainType, scope /*, collection */);
		}

	}

}
