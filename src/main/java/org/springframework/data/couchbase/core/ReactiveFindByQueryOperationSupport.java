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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;

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
		return new ReactiveFindByQuerySupport<>(template, domainType, domainType, ALL_QUERY,
				QueryScanConsistency.NOT_BOUNDED, null, null, template.support());
	}

	static class ReactiveFindByQuerySupport<T> implements ReactiveFindByQuery<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String collection;
		private final String[] distinctFields;
		private final ReactiveTemplateSupport support;

		ReactiveFindByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final Query query, final QueryScanConsistency scanConsistency,
				final String collection, final String[] distinctFields, final ReactiveTemplateSupport support) {
			this.support = support;
			Assert.notNull(domainType, "domainType must not be null!");
			Assert.notNull(returnType, "returnType must not be null!");

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.collection = collection;
			this.distinctFields = distinctFields;
		}

		@Override
		public FindByQueryWithQuery<T> matching(Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) {
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanCons, collection,
					distinctFields, support);
		}

		@Override
		public FindByQueryInCollection<T> inCollection(String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields, support);
		}

		@Override
		@Deprecated
		public FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields, support);
		}

		@Override
		public FindByQueryWithConsistency<T> withConsistency(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields, support);
		}

		@Override
		public <R> FindByQueryWithConsistency<R> as(Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields, support);
		}

		@Override
		public FindByQueryWithDistinct<T> distinct(String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields, support);
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
				String statement = assembleEntityQuery(false, distinctFields);
				Mono<ReactiveQueryResult> allResult = this.collection == null
						? template.getCouchbaseClientFactory().getCluster().reactive().query(statement,
								query.buildQueryOptions(scanConsistency))
						: template.getCouchbaseClientFactory().getScope().reactive().query(statement,
								query.buildQueryOptions(scanConsistency));
				return allResult.onErrorMap(throwable -> {
					if (throwable instanceof RuntimeException) {
						return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
					} else {
						return throwable;
					}
				}).flatMapMany(ReactiveQueryResult::rowsAsObject).flatMap(row -> {
					String id = "";
					long cas = 0;
					if (distinctFields == null) {
						if (row.getString(TemplateUtils.SELECT_ID) == null) {
							throw new CouchbaseException(
									"query did not project " + TemplateUtils.SELECT_ID + ". Either use #{#n1ql.selectEntity} or project "
											+ TemplateUtils.SELECT_ID + " and " + TemplateUtils.SELECT_CAS  + " : " + statement);
						}
						id = row.getString(TemplateUtils.SELECT_ID);
						if (row.getLong(TemplateUtils.SELECT_CAS) == null) {
							throw new CouchbaseException(
									"query did not project " + TemplateUtils.SELECT_CAS + ". Either use #{#n1ql.selectEntity} or project "
											+ TemplateUtils.SELECT_ID + " and " + TemplateUtils.SELECT_CAS + " : " + statement);
						}
						cas = row.getLong(TemplateUtils.SELECT_CAS);
						row.removeKey(TemplateUtils.SELECT_ID);
						row.removeKey(TemplateUtils.SELECT_CAS);
					}
					return support.decodeEntity(id, row.toString(), cas, returnType);
				});
			});
		}

		@Override
		public Mono<Long> count() {
			return Mono.defer(() -> {
				String statement = assembleEntityQuery(true, distinctFields);
				Mono<ReactiveQueryResult> countResult = this.collection == null
						? template.getCouchbaseClientFactory().getCluster().reactive().query(statement,
								query.buildQueryOptions(scanConsistency))
						: template.getCouchbaseClientFactory().getScope().reactive().query(statement,
								query.buildQueryOptions(scanConsistency));
				return countResult.onErrorMap(throwable -> {
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
		} // not efficient, just need the first one

		private String assembleEntityQuery(final boolean count, String[] distinctFields) {
			return query.toN1qlSelectString(template, this.collection, this.domainType, this.returnType, count,
					distinctFields);
		}
	}
}
