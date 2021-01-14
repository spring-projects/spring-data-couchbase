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

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.AnalyticsQuery;
import org.springframework.data.couchbase.core.support.OneAndAllReactive;
import org.springframework.data.couchbase.core.support.WithAnalyticsConsistency;
import org.springframework.data.couchbase.core.support.WithAnalyticsQuery;

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;

public interface ReactiveFindByAnalyticsOperation {

	/**
	 * Queries the analytics service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveFindByAnalytics<T> findByAnalytics(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFindByAnalytics<T> extends OneAndAllReactive {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return a mono with the match if found (an empty one otherwise).
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		Mono<T> one();

		/**
		 * Get the first or no result.
		 *
		 * @return the first or an empty mono if none found.
		 */
		Mono<T> first();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		Flux<T> all();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		Mono<Long> count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		Mono<Boolean> exists();

	}

	interface FindByAnalyticsWithQuery<T> extends TerminatingFindByAnalytics<T>, WithAnalyticsQuery<T> {

		/**
		 * Set the filter for the analytics query to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindByAnalytics<T> matching(AnalyticsQuery query);

	}

	@Deprecated
	interface FindByAnalyticsConsistentWith<T> extends FindByAnalyticsWithQuery<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this analytics query.
		 */
		@Deprecated
		FindByAnalyticsWithQuery<T> consistentWith(AnalyticsScanConsistency scanConsistency);

	}

	interface FindByAnalyticsWithConsistency<T> extends FindByAnalyticsConsistentWith<T>, WithAnalyticsConsistency<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this analytics query.
		 */
		FindByAnalyticsWithQuery<T> withConsistency(AnalyticsScanConsistency scanConsistency);

	}

	interface ReactiveFindByAnalytics<T> extends FindByAnalyticsWithConsistency<T> {}

}
