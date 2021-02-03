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

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.OneAndAllExistsReactive;
import org.springframework.data.couchbase.core.support.WithExistsOptions;

import com.couchbase.client.java.kv.ExistsOptions;
/**
 * Insert Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveExistsByIdOperation {

	/**
	 * Checks if the document exists in the bucket.
	 */
	ReactiveExistsById existsById();

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingExistsById extends OneAndAllExistsReactive {

		/**
		 * Performs the operation on the ID given.
		 *
		 * @param id the ID to perform the operation on.
		 * @return true if the document exists, false otherwise.
		 */
		@Override
		Mono<Boolean> one(String id);

		/**
		 * Performs the operation on the collection of ids.
		 *
		 * @param ids the ids to check.
		 * @return a map consisting of the document IDs as the keys and if they exist as the value.
		 */
		@Override
		Mono<Map<String, Boolean>> all(Collection<String> ids);

	}

	/**
	 * Fluent method to specify options.
	 */
	interface ExistsByIdWithOptions extends TerminatingExistsById, WithExistsOptions {
		/**
		 * Fluent method to specify options to use for execution.
		 *
		 * @param options to use for execution
		 */
		@Override
		TerminatingExistsById withOptions(ExistsOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 */
	interface ExistsByIdInCollection extends ExistsByIdWithOptions, InCollection {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		ExistsByIdWithOptions inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 */
	interface ExistsByIdInScope extends ExistsByIdInCollection, InScope {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		ExistsByIdInCollection inScope(String scope);
	}

	/**
	 * Provides methods for constructing KV exists operations in a fluent way.
	 */
	interface ReactiveExistsById extends ExistsByIdInScope {}

}
