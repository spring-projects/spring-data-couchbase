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

import java.util.List;
import java.util.stream.Stream;

import org.springframework.data.couchbase.core.ReactiveFindByQueryOperationSupport.ReactiveFindByQuerySupport;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * {@link ExecutableFindByQueryOperation} implementations for Couchbase.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class ExecutableFindByQueryOperationSupport implements ExecutableFindByQueryOperation {

	private static final Query ALL_QUERY = new Query();

	private final CouchbaseTemplate template;

	public ExecutableFindByQueryOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindByQuery<T> findByQuery(final Class<T> domainType) {
		return new ExecutableFindByQuerySupport<T>(template, domainType, domainType, ALL_QUERY, null, null, null, null,
				null, null, null);
	}

	static class ExecutableFindByQuerySupport<T> implements ExecutableFindByQuery<T> {

		private final CouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final ReactiveFindByQuerySupport<T> reactiveSupport;
		private final QueryScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final QueryOptions options;
		private final String[] distinctFields;
		private final String[] fields;
		private final CouchbaseStuffHandle txCtx;

		ExecutableFindByQuerySupport(final CouchbaseTemplate template, final Class<?> domainType, final Class<T> returnType,
				final Query query, final QueryScanConsistency scanConsistency, final String scope, final String collection,
				final QueryOptions options, final String[] distinctFields, final String[] fields,
				final CouchbaseStuffHandle txCtx) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.reactiveSupport = new ReactiveFindByQuerySupport<T>(template.reactive(), domainType, returnType, query,
					scanConsistency, scope, collection, options, distinctFields, fields, txCtx,
					new NonReactiveSupportWrapper(template.support()));
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.distinctFields = distinctFields;
			this.fields = fields;
			this.txCtx = txCtx;
		}

		@Override
		public T oneValue() {
			return reactiveSupport.one().block();
		}

		@Override
		public T firstValue() {
			return reactiveSupport.first().block();
		}

		@Override
		public List<T> all() {
			return reactiveSupport.all().collectList().block();
		}

		@Override
		public FindByQueryTxOrNot<T> matching(final Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) {
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanCons, scope, collection,
					options, distinctFields, fields, txCtx);
		}

		@Override
		@Deprecated
		public FindByQueryInScope<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public FindByQueryConsistentWith<T> withConsistency(final QueryScanConsistency scanConsistency) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public <R> FindByQueryWithQuery<R> as(final Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public FindByQueryWithProjection<T> project(String[] fields) {
			Assert.notNull(fields, "Fields must not be null");
			Assert.isNull(distinctFields, "only one of project(fields) and distinct(distinctFields) can be specified");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public FindByQueryWithProjecting<T> distinct(final String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null");
			Assert.isNull(fields, "only one of project(fields) and distinct(distinctFields) can be specified");
			// Coming from an annotation, this cannot be null.
			// But a non-null but empty distinctFields means distinct on all fields
			// So to indicate do not use distinct, we use {"-"} from the annotation, and here we change it to null.
			String[] dFields = distinctFields.length == 1 && "-".equals(distinctFields[0]) ? null : distinctFields;
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, dFields, fields, txCtx);
		}

		@Override
		public FindByQueryWithDistinct<T> transaction(CouchbaseStuffHandle txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null!");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public Stream<T> stream() {
			return reactiveSupport.all().toStream();
		}

		@Override
		public long count() {
			Long l = reactiveSupport.count().block();
			if (l == null) {
				throw new CouchbaseQueryExecutionException("count query did not return a count : " + query.export());
			}
			return l;
		}

		@Override
		public boolean exists() {
			return count() > 0;
		}

		@Override
		public TerminatingFindByQuery<T> withOptions(final QueryOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public FindByQueryInCollection<T> inScope(final String scope) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

		@Override
		public FindByQueryWithDistinct<T> inCollection(final String collection) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options, distinctFields, fields, txCtx);
		}

	}

}
