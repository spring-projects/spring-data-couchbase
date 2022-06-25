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

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.WithConsistency;
import org.springframework.data.couchbase.core.support.WithQuery;
import org.springframework.data.couchbase.core.support.WithQueryOptions;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * RemoveBy Query Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableRemoveByQueryOperation {

	/**
	 * Remove via the query service.
	 */
	<T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingRemoveByQuery<T> {

		/**
		 * Remove all matching documents.
		 *
		 * @return RemoveResult for each matching document
		 */
		List<RemoveResult> all();

	}

	/**
	 * Fluent methods to specify the query
	 *
	 * @param <T> the entity type.
	 */
	interface RemoveByQueryWithQuery<T> extends TerminatingRemoveByQuery<T>, WithQuery<T> {

		TerminatingRemoveByQuery<T> matching(Query query);

		default TerminatingRemoveByQuery<T> matching(QueryCriteriaDefinition criteria) {
			return matching(Query.query(criteria));
		}

	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RemoveByQueryWithOptions<T> extends RemoveByQueryWithQuery<T>, WithQueryOptions<RemoveResult> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options to use for execution
		 */
		RemoveByQueryWithQuery<T> withOptions(QueryOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RemoveByQueryInCollection<T> extends RemoveByQueryWithOptions<T>, InCollection<Object> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		RemoveByQueryWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface RemoveByQueryInScope<T> extends RemoveByQueryInCollection<T>, InScope<Object> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		RemoveByQueryInCollection<T> inScope(String scope);
	}

	@Deprecated
	interface RemoveByQueryConsistentWith<T> extends RemoveByQueryInScope<T> {

		@Deprecated
		RemoveByQueryInScope<T> consistentWith(QueryScanConsistency scanConsistency);

	}

	interface RemoveByQueryWithConsistency<T> extends RemoveByQueryConsistentWith<T>, WithConsistency<T> {
		@Override
		RemoveByQueryConsistentWith<T> withConsistency(QueryScanConsistency scanConsistency);

	}

	/**
	 * Provides methods for constructing query operations in a fluent way.
	 *
	 * @param <T> the entity type.
	 */
	interface ExecutableRemoveByQuery<T> extends RemoveByQueryWithConsistency<T> {}

}
