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

import java.util.List;
import java.util.stream.Stream;

import org.springframework.data.couchbase.core.ReactiveFindByQueryOperationSupport.ReactiveFindByQuerySupport;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.util.Assert;

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
		return new ExecutableFindByQuerySupport<T>(template, domainType, domainType, ALL_QUERY,
				QueryScanConsistency.NOT_BOUNDED, null, null);
	}

	static class ExecutableFindByQuerySupport<T> implements ExecutableFindByQuery<T> {

		private final CouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final ReactiveFindByQuerySupport<T> reactiveSupport;
		private final QueryScanConsistency scanConsistency;
		private final String collection;
		private final String[] distinctFields;

		ExecutableFindByQuerySupport(final CouchbaseTemplate template, final Class<?> domainType, final Class<T> returnType,
				final Query query, final QueryScanConsistency scanConsistency, final String collection,
				final String[] distinctFields) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.reactiveSupport = new ReactiveFindByQuerySupport<T>(template.reactive(), domainType, returnType, query,
					scanConsistency, collection, distinctFields);
			this.scanConsistency = scanConsistency;
			this.collection = collection;
			this.distinctFields = distinctFields;
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
		public TerminatingFindByQuery<T> matching(final Query query) {
			QueryScanConsistency scanCons;
			if (query.getScanConsistency() != null) {
				scanCons = query.getScanConsistency();
			} else {
				scanCons = scanConsistency;
			}
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanCons, collection,
					distinctFields);
		}

		@Override
		@Deprecated
		public FindByQueryInCollection<T> consistentWith(final QueryScanConsistency scanConsistency) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		public FindByQueryConsistentWith<T> withConsistency(final QueryScanConsistency scanConsistency) {
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		public FindByQueryWithConsistency<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		public <R> FindByQueryWithConsistency<R> as(final Class<R> resturnType) {
			Assert.notNull(resturnType, "returnType must not be null!");
			return new ExecutableFindByQuerySupport<>(template, domainType, resturnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		public FindByQueryWithProjection<T> distinct(final String[] distinctFields) {
			Assert.notNull(distinctFields, "distinctFields must not be null!");
			return new ExecutableFindByQuerySupport<>(template, domainType, returnType, query, scanConsistency, collection,
					distinctFields);
		}

		@Override
		public Stream<T> stream() {
			return reactiveSupport.all().toStream();
		}

		@Override
		public long count() {
			return reactiveSupport.count().block();
		}

		@Override
		public boolean exists() {
			return count() > 0;
		}
	}

}
