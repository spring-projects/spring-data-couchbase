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
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.AnalyticsQuery;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAll;
import org.springframework.data.couchbase.core.support.WithAnalyticsConsistency;
import org.springframework.data.couchbase.core.support.WithAnalyticsOptions;
import org.springframework.data.couchbase.core.support.WithAnalyticsQuery;
import org.springframework.lang.Nullable;

import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsScanConsistency;

/**
 * FindByAnalytics Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */public interface ExecutableFindByAnalyticsOperation {

	/**
	 * Queries the analytics service.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ExecutableFindByAnalytics<T> findByAnalytics(Class<T> domainType);

	interface TerminatingFindByAnalytics<T> extends OneAndAll<T> {

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		default Optional<T> one() {
			return Optional.ofNullable(oneValue());
		}

		/**
		 * Get exactly zero or one result.
		 *
		 * @return {@literal null} if no match found.
		 * @throws IncorrectResultSizeDataAccessException if more than one match found.
		 */
		@Nullable
		T oneValue();

		/**
		 * Get the first or no result.
		 *
		 * @return {@link Optional#empty()} if no match found.
		 */
		default Optional<T> first() {
			return Optional.ofNullable(firstValue());
		}

		/**
		 * Get the first or no result.
		 *
		 * @return {@literal null} if no match found.
		 */
		@Nullable
		T firstValue();

		/**
		 * Get all matching elements.
		 *
		 * @return never {@literal null}.
		 */
		List<T> all();

		/**
		 * Stream all matching elements.
		 *
		 * @return a {@link Stream} of results. Never {@literal null}.
		 */
		Stream<T> stream();

		/**
		 * Get the number of matching elements.
		 *
		 * @return total number of matching elements.
		 */
		long count();

		/**
		 * Check for the presence of matching elements.
		 *
		 * @return {@literal true} if at least one matching element exists.
		 */
		boolean exists();

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

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use.
	 */
	interface FindByAnalyticsWithOptions<T> extends FindByAnalyticsWithQuery<T>, WithAnalyticsOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options to use for execution
		 */
		@Override
		FindByAnalyticsWithQuery<T> withOptions(AnalyticsOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByAnalyticsInCollection<T> extends FindByAnalyticsWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		FindByAnalyticsWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByAnalyticsInScope<T> extends FindByAnalyticsInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		FindByAnalyticsInCollection<T> inScope(String scope);
	}

	@Deprecated
	interface FindByAnalyticsConsistentWith<T> extends FindByAnalyticsInScope<T> {

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
		FindByAnalyticsConsistentWith<T> withConsistency(AnalyticsScanConsistency scanConsistency);
	}

	/**
	 * Result type override (Optional).
	 */
	interface FindByAnalyticsWithProjection<T> extends FindByAnalyticsWithConsistency<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link FindByAnalyticsWithConsistency}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> FindByAnalyticsWithConsistency<R> as(Class<R> returnType);
	}

	interface ExecutableFindByAnalytics<T> extends FindByAnalyticsWithProjection<T> {}

}
