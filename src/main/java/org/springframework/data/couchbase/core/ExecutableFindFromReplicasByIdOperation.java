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

import java.util.Collection;

import org.springframework.data.couchbase.core.support.AnyId;
import org.springframework.data.couchbase.core.support.InCollection;
import org.springframework.data.couchbase.core.support.InScope;
import org.springframework.data.couchbase.core.support.WithGetAnyReplicaOptions;

import com.couchbase.client.java.kv.GetAnyReplicaOptions;

/**
 * Query Operations
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ExecutableFindFromReplicasByIdOperation {

	/**
	 * Loads a document from a replica.
	 *
	 * @param domainType the entity type to use for the results.
	 */
	<T> ExecutableFindFromReplicasById<T> findFromReplicasById(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual get execution.
	 */
	interface TerminatingFindFromReplicasById<T> extends AnyId<T> {
		/**
		 * Finds one document based on the given ID.
		 *
		 * @param id the document ID.
		 * @return the entity if found.
		 */
		@Override
		T any(String id);
		/**
		 * Finds a list of documents based on the given IDs.
		 *
		 * @param ids the document ID ids.
		 * @return the list of found entities.
		 */
		@Override
		Collection<? extends T> any(Collection<String> ids);

	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindFromReplicasByIdWithOptions<T> extends TerminatingFindFromReplicasById<T>, WithGetAnyReplicaOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options options to use for execution
		 */
		@Override
		TerminatingFindFromReplicasById<T> withOptions(GetAnyReplicaOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindFromReplicasByIdInCollection<T> extends FindFromReplicasByIdWithOptions<T>, InCollection<T> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		FindFromReplicasByIdWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface FindFromReplicasByIdInScope<T> extends FindFromReplicasByIdInCollection<T>, InScope<T> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		FindFromReplicasByIdInCollection<T> inScope(String scope);
	}

	/**
	 * Provides methods for constructing get operations in a fluent way.
	 *
	 * @param <T> the entity type to use for the results
	 */
	interface ExecutableFindFromReplicasById<T> extends FindFromReplicasByIdInScope<T> {}

}
