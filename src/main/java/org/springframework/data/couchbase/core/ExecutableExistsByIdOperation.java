/*
 * Copyright 2012-2025 the original author or authors
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

import java.util.Collection;
import java.util.Map;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllExists;
import org.springframework.data.couchbase.core.support.WithExistsOptions;

import com.couchbase.client.java.kv.ExistsOptions;

/**
 * Insert Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableExistsByIdOperation {

	/**
	 * Checks if the document exists in the bucket.
	 */
	@Deprecated
	ExecutableExistsById existsById();

	/**
	 * Checks if the document exists in the bucket.
	 */
	ExecutableExistsById existsById(Class<?> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingExistsById extends OneAndAllExists {

		/**
		 * Performs the operation on the ID given.
		 *
		 * @param id the ID to perform the operation on.
		 * @return true if the document exists, false otherwise.
		 */
		@Override
		boolean one(String id);

		/**
		 * Performs the operation on the collection of ids.
		 *
		 * @param ids the ids to check.
		 * @return a map consisting of the document IDs as the keys and if they exist as the value.
		 */
		@Override
		Map<String, Boolean> all(Collection<String> ids);
	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface ExistsByIdWithOptions<T> extends TerminatingExistsById, WithExistsOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options options to use for execution
		 */
		@Override
		TerminatingExistsById withOptions(ExistsOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface ExistsByIdInCollection<T> extends ExistsByIdWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		ExistsByIdWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface ExistsByIdInScope<T> extends ExistsByIdInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		ExistsByIdInCollection<T> inScope(String scope);
	}

	/**
	 * Provides methods for constructing KV exists operations in a fluent way.
	 */
	interface ExecutableExistsById extends ExistsByIdInScope {}

}
