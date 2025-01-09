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

import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.core.support.TemplateUtils;
import org.springframework.util.Assert;

import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.transactions.TransactionQueryOptions;
import com.couchbase.client.java.transactions.TransactionQueryResult;

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
		return new ReactiveFindByQuerySupport<>(template, domainType, domainType, ALL_QUERY, null,
				OptionsBuilder.getScopeFrom(domainType), OptionsBuilder.getCollectionFrom(domainType), null, null, null,
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
		private final String[] fields;
		private final QueryOptions options;
		private final ReactiveTemplateSupport support;

		ReactiveFindByQuerySupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final Query query, final QueryScanConsistency scanConsistency,
				final String scope, final String collection, final QueryOptions options, final String[] distinctFields,
				final String[] fields, final ReactiveTemplateSupport support) {
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
			this.fields = fields;
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
					options, distinctFields, fields, support);
		}

		@Override
		public TerminatingFindByQuery<T> withOptions(final QueryOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, support);
		}

		@Override
		public FindByQueryInCollection<T> inScope(final String scope) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency,
					scope != null ? scope : this.scope, collection, options, distinctFields, fields, support);
		}

		@Override
		public FindByQueryWithConsistency<T> inCollection(final String collection) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection != null ? collection : this.collection, options, distinctFields, fields, support);
		}

		@Override
		@Deprecated
		public FindByQueryConsistentWith<T> consistentWith(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, support);
		}

		@Override
		public FindByQueryWithConsistency<T> withConsistency(QueryScanConsistency scanConsistency) {
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, support);
		}

		public <R> FindByQueryWithConsistency<R> as(Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, support);
		}

		@Override
		public FindByQueryWithProjection<T> project(String[] fields) {
			Assert.notNull(fields, "Fields must not be null");
			Assert.isNull(distinctFields, "only one of project(fields) and distinct(distinctFields) can be specified");
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, support);
		}

		@Override
		public FindByQueryWithDistinct<T> distinct(final String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null");
			Assert.isNull(fields, "only one of project(fields) and distinct(distinctFields) can be specified");
			// Coming from an annotation, this cannot be null.
			// But a non-null but empty distinctFields means distinct on all fields
			// So to indicate do not use distinct, we use {"-"} from the annotation, and here we change it to null.
			String[] dFields = distinctFields.length == 1 && "-".equals(distinctFields[0]) ? null : distinctFields;
			return new ReactiveFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, dFields, fields, support);
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
			String statement = assembleEntityQuery(false, distinctFields, pArgs.getScope(), pArgs.getCollection());
			if (LOG.isDebugEnabled()) {
				LOG.debug("findByQuery {} statement: {}", pArgs, statement);
			}
			CouchbaseClientFactory clientFactory = template.getCouchbaseClientFactory();
			ReactiveScope rs = clientFactory.withScope(pArgs.getScope()).getScope().reactive();

			Mono<Object> allResult = TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(s -> {
				if (!s.isPresent()) {
					QueryOptions opts = buildOptions(pArgs.getOptions());
					return pArgs.getScope() == null ? clientFactory.getCluster().reactive().query(statement, opts)
							: rs.query(statement, opts);
				} else {
					TransactionQueryOptions options = buildTransactionOptions(pArgs.getOptions());
					JsonSerializer jSer = clientFactory.getCluster().environment().jsonSerializer();
					CoreQueryOptions opts = options != null ? options.builder().build() : null;
					return s.get().getCore()
							.queryBlocking(statement,
									pArgs.getScope() == null ? null
											: CoreQueryContext.of(rs.bucketName(), pArgs.getScope()),
									opts, false)
							.map(response -> new TransactionQueryResult(response, jSer));

				}
			});

			return allResult.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException e) {
					return template.potentiallyConvertRuntimeException(e);
				} else {
					return throwable;
				}
			}).flatMapMany(o -> o instanceof ReactiveQueryResult  reactiveQueryResult ? reactiveQueryResult.rowsAsObject()
					: Flux.fromIterable(((TransactionQueryResult) o).rowsAsObject())).flatMap(row -> {
						String id = "";
						Long cas = Long.valueOf(0);
						if (!query.isDistinct() && distinctFields == null) {
							id = row.getString(TemplateUtils.SELECT_ID);
							if (id == null) {
								id = row.getString(TemplateUtils.SELECT_ID_3x);
								row.removeKey(TemplateUtils.SELECT_ID_3x);
							}
							cas = row.getLong(TemplateUtils.SELECT_CAS);
							if (cas == null) {
								cas = row.getLong(TemplateUtils.SELECT_CAS_3x);
								row.removeKey(TemplateUtils.SELECT_CAS_3x);
							}
							row.removeKey(TemplateUtils.SELECT_ID);
							row.removeKey(TemplateUtils.SELECT_CAS);
						}
						return support.decodeEntity(id, row.toString(), cas, returnType, pArgs.getScope(), pArgs.getCollection(),
								null, null);
					});
		}

		public QueryOptions buildOptions(QueryOptions options) {
			QueryScanConsistency qsc = scanConsistency != null ? scanConsistency : template.getConsistency();
			return query.buildQueryOptions(options, qsc).readonly(query.isReadonly());
		}

		private TransactionQueryOptions buildTransactionOptions(QueryOptions options) {
			TransactionQueryOptions opts = OptionsBuilder.buildTransactionQueryOptions(buildOptions(options));
			return opts;
		}

		@Override
		public Mono<Long> count() {
			PseudoArgs<QueryOptions> pArgs = new PseudoArgs(template, scope, collection, options, domainType);
			String statement = assembleEntityQuery(true, distinctFields, pArgs.getScope(), pArgs.getCollection());
			if (LOG.isDebugEnabled()) {
				LOG.debug("findByQuery {} statement: {}", pArgs, statement);
			}

			CouchbaseClientFactory clientFactory = template.getCouchbaseClientFactory();
			ReactiveScope rs = clientFactory.withScope(pArgs.getScope()).getScope().reactive();

			Mono<Object> allResult = TransactionalSupport.checkForTransactionInThreadLocalStorage().flatMap(s -> {
				if (!s.isPresent()) {
					QueryOptions opts = buildOptions(pArgs.getOptions());
					return pArgs.getScope() == null ? clientFactory.getCluster().reactive().query(statement, opts)
							: rs.query(statement, opts);
				} else {
					TransactionQueryOptions options = buildTransactionOptions(pArgs.getOptions());
					JsonSerializer jSer = clientFactory.getCluster().environment().jsonSerializer();
					CoreQueryOptions opts = options != null ? options.builder().build() : null;
					return s.get().getCore()
							.queryBlocking(statement,
									pArgs.getScope() == null ? null
											: CoreQueryContext.of(rs.bucketName(), pArgs.getScope()),
									opts, false)
							.map(response -> new TransactionQueryResult(response, jSer));
				}
			});

			return allResult.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException e) {
					return template.potentiallyConvertRuntimeException(e);
				} else {
					return throwable;
				}
			}).flatMapMany(o -> o instanceof ReactiveQueryResult reactiveQueryResult ? reactiveQueryResult.rowsAsObject()
					: Flux.fromIterable(((TransactionQueryResult) o).rowsAsObject()))
					.map(row -> row.getLong(row.getNames().iterator().next())).next();
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0); // not efficient, just need the first one
		}

		private String assembleEntityQuery(final boolean count, String[] distinctFields, String scope, String collection) {
			return query.toN1qlSelectString(template.getConverter(), template.getBucketName(), scope, collection,
					this.domainType, this.returnType, count,
					query.getDistinctFields() != null ? query.getDistinctFields() : distinctFields, fields);
		}
	}
}
