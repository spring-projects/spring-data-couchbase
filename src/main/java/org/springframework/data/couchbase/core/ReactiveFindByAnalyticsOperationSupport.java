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

import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.AnalyticsQuery;

import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsScanConsistency;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;

public class ReactiveFindByAnalyticsOperationSupport implements ReactiveFindByAnalyticsOperation {

	private static final AnalyticsQuery ALL_QUERY = new AnalyticsQuery();

	private final ReactiveCouchbaseTemplate template;

	public ReactiveFindByAnalyticsOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveFindByAnalytics<T> findByAnalytics(final Class<T> domainType) {
		return new ReactiveFindByAnalyticsSupport<>(template, domainType, domainType, ALL_QUERY,
				null, null, null, null);
	}

	static class ReactiveFindByAnalyticsSupport<T> implements ReactiveFindByAnalytics<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final AnalyticsQuery query;
		private final AnalyticsScanConsistency scanConsistency;
		private final String scope;
		private final String collection;
		private final AnalyticsOptions options;

		ReactiveFindByAnalyticsSupport(final ReactiveCouchbaseTemplate template, final Class<?> domainType,
				final Class<T> returnType, final AnalyticsQuery query, final AnalyticsScanConsistency scanConsistency,
				String scope, String collection, AnalyticsOptions options) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
		}

		@Override
		public TerminatingFindByAnalytics<T> matching(AnalyticsQuery query) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		@Deprecated
		public FindByAnalyticsWithQuery<T> consistentWith(AnalyticsScanConsistency scanConsistency) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsWithQuery<T> withConsistency(AnalyticsScanConsistency scanConsistency) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public <R> FindByAnalyticsWithConsistency<R> as(final Class<R> returnType) {
			Assert.notNull(returnType, "returnType must not be null!");
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
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
				String statement = assembleEntityQuery(false);
				return template.getCouchbaseClientFactory().getCluster().reactive()
						.analyticsQuery(statement, buildAnalyticsOptions()).onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						}).flatMapMany(ReactiveAnalyticsResult::rowsAsObject).map(row -> {
							String id = row.getString("__id");
							long cas = row.getLong("__cas");
							row.removeKey("__id");
							row.removeKey("__cas");
							return template.support().decodeEntity(id, row.toString(), cas, returnType);
						});
			});
		}

		@Override
		public Mono<Long> count() {
			return Mono.defer(() -> {
				String statement = assembleEntityQuery(true);
				return template.getCouchbaseClientFactory().getCluster().reactive()
						.analyticsQuery(statement, buildAnalyticsOptions()).onErrorMap(throwable -> {
							if (throwable instanceof RuntimeException) {
								return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
							} else {
								return throwable;
							}
						}).flatMapMany(ReactiveAnalyticsResult::rowsAsObject).map(row -> row.getLong("__count")).next();
			});
		}

		@Override
		public Mono<Boolean> exists() {
			return count().map(count -> count > 0);
		}

		@Override
		public TerminatingFindByAnalytics<T> withOptions(final AnalyticsOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsInCollection<T> inScope(final String scope) {
			Assert.hasText(scope, "Scope must not be null nor empty.");
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		@Override
		public FindByAnalyticsWithConsistency<T> inCollection(final String collection) {
			Assert.hasText(collection, "Collection must not be null nor empty.");
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, returnType, query, scanConsistency, scope,
					collection, options);
		}

		private String assembleEntityQuery(final boolean count) {
			final String bucket = "`" + template.getBucketName() + "`";

			final StringBuilder statement = new StringBuilder("SELECT ");
			if (count) {
				statement.append("count(*) as __count");
			} else {
				statement.append("meta().id as __id, meta().cas as __cas, ").append(bucket).append(".*");
			}

			final String dataset = template.support().getJavaNameForEntity(domainType);
			statement.append(" FROM ").append(dataset);

			query.appendSort(statement);
			query.appendSkipAndLimit(statement);
			return statement.toString();
		}

		private AnalyticsOptions buildAnalyticsOptions() {
			final AnalyticsOptions options = AnalyticsOptions.analyticsOptions();
			if (scanConsistency != null) {
				options.scanConsistency(scanConsistency);
			}
			return options;
		}
	}

}
