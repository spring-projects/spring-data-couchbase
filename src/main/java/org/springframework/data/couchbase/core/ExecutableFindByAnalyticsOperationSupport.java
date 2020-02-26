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

import org.springframework.data.couchbase.core.query.AnalyticsQuery;

public class ExecutableFindByAnalyticsOperationSupport implements ExecutableFindByAnalyticsOperation {

	private static final AnalyticsQuery ALL_QUERY = new AnalyticsQuery();

	private final CouchbaseTemplate template;

	public ExecutableFindByAnalyticsOperationSupport(final CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ExecutableFindByAnalytics<T> findByAnalytics(final Class<T> domainType) {
		return new ExecutableFindByAnalyticsSupport<>(template, domainType, ALL_QUERY);
	}

	static class ExecutableFindByAnalyticsSupport<T> implements ExecutableFindByAnalytics<T> {

		private final CouchbaseTemplate template;
		private final Class<T> domainType;
		private final ReactiveFindByAnalyticsOperationSupport.ReactiveFindByAnalyticsSupport<T> reactiveSupport;

		ExecutableFindByAnalyticsSupport(final CouchbaseTemplate template, final Class<T> domainType,
				final AnalyticsQuery query) {
			this.template = template;
			this.domainType = domainType;
			this.reactiveSupport = new ReactiveFindByAnalyticsOperationSupport.ReactiveFindByAnalyticsSupport<>(
					template.reactive(), domainType, query);
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
			return new ExecutableFindByAnalyticsSupport<>(template, domainType, query);
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
