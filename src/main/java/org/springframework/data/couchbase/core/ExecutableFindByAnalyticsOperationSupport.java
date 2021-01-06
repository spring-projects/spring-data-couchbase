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

import org.springframework.data.couchbase.core.ReactiveFindByAnalyticsOperationSupport.ReactiveFindByAnalyticsSupport;
import org.springframework.data.couchbase.core.query.AnalyticsQuery;

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;

public class ExecutableFindByAnalyticsOperationSupport implements ExecutableFindByAnalyticsOperation {

	private static final AnalyticsQuery ALL_QUERY = new AnalyticsQuery();

	private final CouchbaseTemplate template;

	public ExecutableFindByAnalyticsOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindByAnalytics<T> findByAnalytics(final Class<T> domainType) {
		return new ExecutableFindByAnalyticsSupport<>(template, domainType, ALL_QUERY,
				AnalyticsScanConsistency.NOT_BOUNDED);
	}

	static class ExecutableFindByAnalyticsSupport<T> implements ExecutableFindByAnalytics<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final ReactiveFindByAnalyticsSupport<T> reactiveSupport;
		private final AnalyticsQuery query;
		private final AnalyticsScanConsistency scanConsistency;

		ExecutableFindByAnalyticsSupport(final CouchbaseTemplate template, final Class<T> domainType,
				final AnalyticsQuery query, final AnalyticsScanConsistency scanConsistency) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.reactiveSupport = new ReactiveFindByAnalyticsSupport<>(template.reactive(), domainType, query,
					scanConsistency);
			this.scanConsistency = scanConsistency;
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
		public TerminatingFindByAnalytics<T> matching(final AnalyticsQuery query) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, query, scanConsistency);
		}

		@Override
		@Deprecated
		public FindByAnalyticsWithQuery<T> consistentWith(final AnalyticsScanConsistency scanConsistency) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, query, scanConsistency);
		}

		@Override
		public FindByAnalyticsWithConsistency<T> withConsistency(final AnalyticsScanConsistency scanConsistency) {
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, query, scanConsistency);
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
