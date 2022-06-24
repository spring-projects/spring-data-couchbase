/*
 * Copyright 2012-2022 the original author or authors
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

import org.springframework.data.couchbase.core.ReactiveFindByAnalyticsOperationSupport.ReactiveFindByAnalyticsSupport;
import org.springframework.data.couchbase.core.query.AnalyticsQuery;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.util.Assert;

import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsScanConsistency;

public class ExecutableFindByAnalyticsOperationSupport implements ExecutableFindByAnalyticsOperation {

	private static final AnalyticsQuery ALL_QUERY = new AnalyticsQuery();

	private final CouchbaseTemplate template;

	public ExecutableFindByAnalyticsOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindByAnalytics<T> findByAnalytics(final Class<T> domainType) {
		return new ExecutableFindByAnalyticsSupport<>(template, domainType, domainType, ALL_QUERY, null,
				OptionsBuilder.getScopeFrom(domainType), OptionsBuilder.getCollectionFrom(domainType), null);
	}

	static class ExecutableFindByAnalyticsSupport<T> implements ExecutableFindByAnalytics<T> {

		private final CouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final ReactiveFindByAnalyticsSupport<T> reactiveSupport;
		private final AnalyticsQuery query;
		private final AnalyticsScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final AnalyticsOptions options;

		ExecutableFindByAnalyticsSupport(final CouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final AnalyticsQuery query, final AnalyticsScanConsistency scanConsistency,
				final String scope, final String collection, final AnalyticsOptions options) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.reactiveSupport = new ReactiveFindByAnalyticsSupport<>(template.reactive(), domainType, returnType, query,
					scanConsistency, scope, collection, options, new NonReactiveSupportWrapper(template.support()));
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
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
		public FindByAnalyticsWithConsistency<T> matching(final AnalyticsQuery query) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsWithQuery<T> withOptions(final AnalyticsOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsInCollection<T> inScope(final String scope) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency,
					scope != null ? scope : this.scope, collection, options);
		}

		@Override
		public FindByAnalyticsWithProjection<T> inCollection(final String collection) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection != null ? collection : this.collection, options);
		}

		@Override
		@Deprecated
		public FindByAnalyticsWithQuery<T> consistentWith(final AnalyticsScanConsistency scanConsistency) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsWithConsistency<T> withConsistency(final AnalyticsScanConsistency scanConsistency) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public <R> FindByAnalyticsWithQuery<R> as(final Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
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
