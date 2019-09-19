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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.core.query.AnalyticsQuery;

public interface ReactiveFindByAnalyticsOperation {

	<T> ReactiveFindByAnalytics<T> findByAnalytics(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFindByAnalytics<T> {

		Mono<T> one();

		Mono<T> first();

		Flux<T> all();

		Mono<Long> count();

		Mono<Boolean> exists();

	}

	/**
	 * Terminating operations invoking the actual query execution.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface FindByAnalyticsWithQuery<T> extends TerminatingFindByAnalytics<T> {

		/**
		 * Set the filter query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindByAnalytics}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindByAnalytics<T> matching(AnalyticsQuery query);

	}

	interface ReactiveFindByAnalytics<T> extends FindByAnalyticsWithQuery<T> {}

}
