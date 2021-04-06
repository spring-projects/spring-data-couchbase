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

import com.couchbase.client.java.query.ReactiveQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.AnalyticsQuery;
import org.springframework.data.couchbase.core.support.TemplateUtils;

import com.couchbase.client.core.error.CouchbaseException;
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
		return new ReactiveFindByAnalyticsSupport<>(template, domainType, ALL_QUERY, AnalyticsScanConsistency.NOT_BOUNDED,
				template.support());
	}

	static class ReactiveFindByAnalyticsSupport<T> implements ReactiveFindByAnalytics<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final AnalyticsQuery query;
		private final AnalyticsScanConsistency scanConsistency;
		private final ReactiveTemplateSupport support;

		ReactiveFindByAnalyticsSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType,
				final AnalyticsQuery query, final AnalyticsScanConsistency scanConsistency, ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.scanConsistency = scanConsistency;
			this.support = support;
		}

		@Override
		public TerminatingFindByAnalytics<T> matching(AnalyticsQuery query) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, query, scanConsistency, support);
		}

		@Override
		@Deprecated
		public FindByAnalyticsWithQuery<T> consistentWith(AnalyticsScanConsistency scanConsistency) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, query, scanConsistency, support);
		}

		@Override
		public FindByAnalyticsWithQuery<T> withConsistency(AnalyticsScanConsistency scanConsistency) {
			return new ReactiveFindByAnalyticsSupport<>(template, domainType, query, scanConsistency, support);
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
						}).flatMapMany(ReactiveAnalyticsResult::rowsAsObject).flatMap(row -> {
							String id = "";
							long cas = 0;
							if (row.getString(TemplateUtils.SELECT_ID) == null) {
								return Flux.error(new CouchbaseException("analytics query did not project " + TemplateUtils.SELECT_ID
										+ ". Either use #{#n1ql.selectEntity} or project " + TemplateUtils.SELECT_ID + " and "
										+ TemplateUtils.SELECT_CAS + " : " + statement));
							}
							id = row.getString(TemplateUtils.SELECT_ID);
							if (row.getLong(TemplateUtils.SELECT_CAS) == null) {
								return Flux.error(new CouchbaseException("analytics query did not project " + TemplateUtils.SELECT_CAS
										+ ". Either use #{#n1ql.selectEntity} or project " + TemplateUtils.SELECT_ID + " and "
										+ TemplateUtils.SELECT_CAS + " : " + statement));
							}
							cas = row.getLong(TemplateUtils.SELECT_CAS);
							row.removeKey(TemplateUtils.SELECT_ID);
							row.removeKey(TemplateUtils.SELECT_CAS);
							return template.support().decodeEntity(id, row.toString(), cas, domainType);
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

		private String assembleEntityQuery(final boolean count) {
			final String bucket = "`" + template.getBucketName() + "`";

			final StringBuilder statement = new StringBuilder("SELECT ");
			if (count) {
				statement.append("count(*) as __count");
			} else {
				statement.append("meta().id as __id, meta().cas as __cas, ").append(bucket).append(".*");
			}

			final String dataset = support.getJavaNameForEntity(domainType);
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
