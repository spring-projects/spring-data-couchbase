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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.query.QueryOptions;
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
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveFindByQueryOperationSupport.class);

	public ReactiveFindByQueryOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindByQuery<T> findByQuery(final Class<T> domainType) {
		return new ReactiveFindByQuerySupport<>(template, domainType, domainType, ALL_QUERY, null, null, null, null, null,
				template.support());
	}

	static class ReactiveFindByQuerySupport<T> implements ReactiveFindByQuery<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final QueryScanConsistency scanConsistency;
		private final String collection;
		private final String scope;
		private final String[] distinctFields;
		private final QueryOptions options;
		private final ReactiveTemplateSupport support;

		ReactiveFindByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final Query query, final QueryScanConsistency scanConsistency, final String scope,
				final String collection, final QueryOptions options, final String[] distinctFields,
				final ReactiveTemplateSupport support) {
			Assert.notNull(domainType, "domainType must not be null!");
			Assert.notNull(returnType, "returnType must not be null!");
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.distinctFields = distinctFields;
			this.support = support;
		}

		@Override
		public FindByQueryWithQuery<T> matching(Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) { // redundant, since buildQueryOptions() will use
																								// query.getScanConsistency()
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanCons, scope, collection,
					options, distinctFields, support);
		}

		@Override
		public TerminatingFindByQuery<T> withOptions(final QueryOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		public FindByQueryInCollection<T> inScope(final String scope) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		public FindByQueryWithConsistency<T> inCollection(final String collection) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		@Deprecated
		public FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		public FindByQueryWithConsistency<T> withConsistency(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		public <R> FindByQueryWithConsistency<R> as(Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, support);
		}

		@Override
		public FindByQueryWithDistinct<T> distinct(final String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null!");
			// Coming from an annotation, this cannot be null.
			// But a non-null but empty distinctFields means distinct on all fields
			// So to indicate do not use distinct, we use {"-"} from the annotation, and here we change it to null.
			String[] dFields = distinctFields.length == 1 && "-".equals(distinctFields[0]) ? null : distinctFields;
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, dFields, support);
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
			PseudoArgs<QueryOptions> pArgs = new PseudoArgs(template, scope, collection, options, domainType);
			String statement = assembleEntityQuery(false, distinctFields, pArgs.getCollection());
			LOG.trace("findByQuery {} statement: {}", pArgs, statement);
			Mono<ReactiveQueryResult> allResult = pArgs.getScope() == null
					? template.getCouchbaseClientFactory().getCluster().reactive().query(statement,
							buildOptions(pArgs.getOptions()))
					: template.getCouchbaseClientFactory().withScope(pArgs.getScope()).getScope().reactive().query(statement,
							buildOptions(pArgs.getOptions()));
			return Flux.defer(() -> allResult.onErrorMap(throwable -> {
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
						return Flux.error(new CouchbaseException(
								"query did not project " + TemplateUtils.SELECT_ID + ". Either use #{#n1ql.selectEntity} or project "
										+ TemplateUtils.SELECT_ID + " and " + TemplateUtils.SELECT_CAS + " : " + statement));
					}
					id = row.getString(TemplateUtils.SELECT_ID);
					if (row.getLong(TemplateUtils.SELECT_CAS) == null) {
						return Flux.error(new CouchbaseException(
								"query did not project " + TemplateUtils.SELECT_CAS + ". Either use #{#n1ql.selectEntity} or project "
										+ TemplateUtils.SELECT_ID + " and " + TemplateUtils.SELECT_CAS + " : " + statement));
					}
					cas = row.getLong(TemplateUtils.SELECT_CAS);
					row.removeKey(TemplateUtils.SELECT_ID);
					row.removeKey(TemplateUtils.SELECT_CAS);
				}
				return support.decodeEntity(id, row.toString(), cas, returnType);
			}));
		}

		private QueryOptions buildOptions(QueryOptions options) {
			QueryOptions opts = query.buildQueryOptions(options, scanConsistency);
			return opts;
		}

		@Override
		public Mono<Long> count() {
			PseudoArgs<QueryOptions> pArgs = new PseudoArgs(template, scope, collection, options, domainType);
			String statement = assembleEntityQuery(true, distinctFields, pArgs.getCollection());
			LOG.trace("findByQuery {} statement: {}", pArgs, statement);
			Mono<ReactiveQueryResult> countResult = pArgs.getScope() == null
					? template.getCouchbaseClientFactory().getCluster().reactive().query(statement,
							buildOptions(pArgs.getOptions()))
					: template.getCouchbaseClientFactory().withScope(pArgs.getScope()).getScope().reactive().query(statement,
							buildOptions(pArgs.getOptions()));
			return Mono.defer(() -> countResult.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			}).flatMapMany(ReactiveQueryResult::rowsAsObject).map(row -> row.getLong(TemplateUtils.SELECT_COUNT)).next());
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0); // not efficient, just need the first one
		}

		private String assembleEntityQuery(final boolean count, String[] distinctFields, String collection) {
			return query.toN1qlSelectString(template, collection, this.domainType, this.returnType, count, distinctFields);
		}
	}
}
