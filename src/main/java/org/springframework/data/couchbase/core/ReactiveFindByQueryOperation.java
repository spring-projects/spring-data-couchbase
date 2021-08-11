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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllReactive;
import org.springframework.data.couchbase.core.support.WithConsistency;
import org.springframework.data.couchbase.core.support.WithDistinct;
import org.springframework.data.couchbase.core.support.WithQuery;
import org.springframework.data.couchbase.core.support.WithQueryOptions;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * ReactiveFindByQueryOperation<br>
 * Queries the N1QL service.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public interface ReactiveFindByQueryOperation {

	/**
	 * Create the operation for the domainType
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ReactiveFindByQuery<T> findByQuery(Class<T> domainType);

	/**
	 * Compose find execution by calling one of the terminating methods.
	 */
	interface TerminatingFindByQuery<T> extends OneAndAllReactive<T> {

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

		QueryOptions buildOptions(QueryOptions options);

	}

	/**
	 * Fluent methods to filter by query
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithQuery<T> extends TerminatingFindByQuery<T>, WithQuery<T> {

		/**
		 * Set the filter {@link Query} to be used.
		 *
		 * @param query must not be {@literal null}.
		 * @throws IllegalArgumentException if query is {@literal null}.
		 */
		TerminatingFindByQuery<T> matching(Query query);

		/**
		 * Set the filter {@link QueryCriteriaDefinition criteria} to be used.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return new instance of {@link TerminatingFindByQuery}.
		 * @throws IllegalArgumentException if criteria is {@literal null}.
		 */
		default TerminatingFindByQuery<T> matching(QueryCriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

	}

	/**
	 * Fluent method to specify options.
	 * 
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithOptions<T> extends FindByQueryWithQuery<T>, WithQueryOptions<T> {
		/**
		 * @param options options to use for execution
		 */
		TerminatingFindByQuery<T> withOptions(QueryOptions options);
	}

	/**
	 * Fluent method to specify the collection
	 * 
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryInCollection<T> extends FindByQueryWithOptions<T>, InCollection<T> {
		FindByQueryWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryInScope<T> extends FindByQueryInCollection<T>, InScope<T> {
		FindByQueryInCollection<T> inScope(String scope);
	}

	/**
	 * To be removed at the next major release. use WithConsistency instead
	 *
	 * @param <T> the entity type to use for the results.
	 */
	@Deprecated
	interface FindByQueryConsistentWith<T> extends FindByQueryInScope<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		@Deprecated
		FindByQueryInScope<T> consistentWith(QueryScanConsistency scanConsistency);

	}

	/**
	 * Fluent method to specify scan consistency.  Scan consistency may also come from an annotation.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithConsistency<T> extends FindByQueryConsistentWith<T>, WithConsistency<T> {

		/**
		 * Allows to override the default scan consistency.
		 *
		 * @param scanConsistency the custom scan consistency to use for this query.
		 */
		FindByQueryConsistentWith<T> withConsistency(QueryScanConsistency scanConsistency);

	}

	/**
	 * Fluent method to specify a return type different than the the entity type to use for the results.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithProjection<T> extends FindByQueryWithConsistency<T> {

		/**
		 * Define the target type fields should be mapped to. <br />
		 * Skip this step if you are anyway only interested in the original domain type.
		 *
		 * @param returnType must not be {@literal null}.
		 * @return new instance of {@link FindByQueryWithProjection}.
		 * @throws IllegalArgumentException if returnType is {@literal null}.
		 */
		<R> FindByQueryWithConsistency<R> as(Class<R> returnType);
	}

	/**
	 * Fluent method to specify DISTINCT fields
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindByQueryWithDistinct<T> extends FindByQueryWithProjection<T>, WithDistinct<T> {

		/**
		 * Finds the distinct values for a specified {@literal field} across a single {@link } or view.
		 *
		 * @param distinctFields name of the field. Must not be {@literal null}.
		 * @return new instance of {@link ReactiveFindByQuery}.
		 * @throws IllegalArgumentException if field is {@literal null}.
		 */
		FindByQueryWithProjection<T> distinct(String[] distinctFields);
	}

	/**
	 * provides methods for constructing query operations in a fluent way.
	 *
	 * @param <T> the entity type to use for the results
	 */
	interface ReactiveFindByQuery<T> extends FindByQueryWithDistinct<T> {}

}
