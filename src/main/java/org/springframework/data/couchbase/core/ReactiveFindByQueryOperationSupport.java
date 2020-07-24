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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.TemplateUtils;

import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;

import java.util.Collection;

import static com.couchbase.client.java.kv.GetOptions.getOptions;

/**
 * {@link ReactiveFindByQueryOperation} implementations for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class ReactiveFindByQueryOperationSupport implements ReactiveFindByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final ReactiveCouchbaseTemplate template;

	public ReactiveFindByQueryOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindByQuery<T> findByQuery(final Class<T> domainType) {
		return new ReactiveFindByQuerySupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED,
				null /*"default:`"+template.getCouchbaseClientFactory().getBucket().name()+"`" */, null);
	}

	static class ReactiveFindByQuerySupport<T> implements ReactiveFindByQuery<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String scope;
		private final String collection;

		ReactiveFindByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final Query query,
				final QueryScanConsistency scanConsistency, final String scope, final String collection) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
		}

		@Override
		public TerminatingFindByQuery<T> matching(Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) {
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ReactiveFindByQuerySupport<>(template, domainType, query, scanCons,
					scope, collection);
		}

		@Override
		public FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, query, scanConsistency,
					scope, collection);
		}

		@Override
		public FindInScope<T> inScope(String scope) {
			return new ReactiveFindByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection);
		}
		@Override
		public FindInScope<T> inCollection(String scope, String collection) {
			return new ReactiveFindByQuerySupport<>(template, domainType, query, scanConsistency, scope, collection);
		}

		@Override
		public TerminatingDistinct<Object> distinct(String field) {
			throw new RuntimeException(("not implemented"));
		}

		@Override
		public Mono<T> one() {
			return all().singleOrEmpty();
		}

		@Override
		public Mono<T> first() {
			return all().next();
		}

		@Override
		public Flux<T> all() {
			return Flux.defer(() -> {
				String statement = assembleEntityQuery(false, scope, collection);
				// these should all use getScope().reactive()
				// any point in using withScope() here?
				//return template.getCouchbaseClientFactory().withScope(query.getScope()).getScope().reactive() // getCluster().reactive()
				return template.getCouchbaseClientFactory().getCluster().reactive() // getCluster().reactive()

						.query(statement, query.buildQueryOptions(scanConsistency,
								scope)).onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						}).flatMapMany(ReactiveQueryResult::rowsAsObject).map(row -> {
							String id = row.getString(TemplateUtils.SELECT_ID);
							long cas = row.getLong(TemplateUtils.SELECT_CAS);
							row.removeKey(TemplateUtils.SELECT_ID);
							row.removeKey(TemplateUtils.SELECT_CAS);
							return template.support().decodeEntity(id, row.toString(), cas, domainType);
						});
			});
		}

		@Override
		public Mono<T> one(final String id) {
			return Mono.just(id).flatMap(docId -> {
				GetOptions options = getOptions().transcoder(RawJsonTranscoder.INSTANCE);
				// TODO if (fields != null && !fields.isEmpty()) {
				// TODO 	options.project(fields);
				// TODO }
				return template.getCollection(collection).reactive().get(docId, options);
			}).map(result -> template.support().decodeEntity(id, result.contentAs(String.class), result.cas(), domainType))
					.onErrorResume(throwable -> {
						if (throwable instanceof RuntimeException) {
							if (throwable instanceof DocumentNotFoundException) {
								return Mono.empty();
							}
						}
						return Mono.error(throwable);
					}).onErrorMap(throwable -> {
						if (throwable instanceof RuntimeException) {
							return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
						} else {
							return throwable;
						}
					});
		}

		@Override
		public Flux<T> all(final Collection<String> ids) {
			return Flux.fromIterable(ids).flatMap(this::one);
		}

		@Override
		public Mono<Long> count() {
			return Mono.defer(() -> {
				String statement = assembleEntityQuery(true, scope, collection);
				return template.getCouchbaseClientFactory().getCluster().reactive()
						.query(statement, query.buildQueryOptions(scanConsistency,
								scope)).onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						}).flatMapMany(ReactiveQueryResult::rowsAsObject).map(row -> {
							return row.getLong(TemplateUtils.SELECT_COUNT);
						}).next();
			});
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0);
		}

		private String assembleEntityQuery(final boolean count, String scope, String collection) {
			return query.toN1qlSelectString(template, this.domainType, count , scope, collection);
		}
	}

}
