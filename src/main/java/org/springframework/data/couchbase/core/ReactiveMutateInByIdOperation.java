/*
 * Copyright 2012-2023 the original author or authors
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

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.core.support.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

/**
 * Mutate In Operations
 *
 * @author Tigran Babloyan
 * @since 5.1
 */
public interface ReactiveMutateInByIdOperation {

	/**
	 * Mutate using the KV service.
	 *
	 * @param domainType the entity type to mutate.
	 */
	<T> ReactiveMutateInById<T> mutateInById(Class<T> domainType);

	/**
	 * Terminating operations invoking the actual execution.
	 */
	interface TerminatingMutateInById<T> extends OneAndAllEntityReactive<T> {

		/**
		 * Mutate one entity.
		 *
		 * @return Upserted entity.
		 */
		@Override
		Mono<T> one(T object);

		/**
		 * Mutate a collection of entities.
		 *
		 * @return Inserted entities
		 */
		@Override
		Flux<? extends T> all(Collection<? extends T> objects);

	}

	interface MutateInByIdWithPaths<T> extends TerminatingMutateInById<T>, WithMutateInPaths<T> {
		/**
		 * Adds given paths to remove mutations.
		 * See {@link com.couchbase.client.java.kv.Remove} for more details.
		 * @param removePaths The property paths to removed from document.
		 */
		@Override
		MutateInByIdWithPaths<T> withRemovePaths(final String... removePaths);
		/**
		 * Adds given paths to insert mutations.
		 * See {@link com.couchbase.client.java.kv.Insert} for more details.
		 * @param insertPaths The property paths to be inserted into the document.
		 */
		@Override
		MutateInByIdWithPaths<T> withInsertPaths(final String... insertPaths);
		/**
		 * Adds given paths to upsert mutations.
		 * See {@link com.couchbase.client.java.kv.Upsert} for more details.
		 * @param upsertPaths The property paths to be upserted into the document.
		 */
		@Override
		MutateInByIdWithPaths<T> withUpsertPaths(final String... upsertPaths);
		/**
		 * Adds given paths to replace mutations.
		 * See {@link com.couchbase.client.java.kv.Replace} for more details.
		 * @param replacePaths The property paths to be replaced in the document.
		 */
		@Override
		MutateInByIdWithPaths<T> withReplacePaths(final String... replacePaths);
		/**
		 * Marks that the CAS value should be provided with the mutations to protect against concurrent modifications.
		 * By default the CAS value is not provided.
		 */
		MutateInByIdWithPaths<T> withCasProvided();
	}

	/**
	 * Fluent method to specify options.
	 *
	 * @param <T> the entity type to use.
	 */
	interface MutateInByIdWithOptions<T> extends MutateInByIdWithPaths<T>, WithMutateInOptions<T> {
		/**
		 * Fluent method to specify options to use for execution
		 *
		 * @param options to use for execution
		 */
		@Override
		TerminatingMutateInById<T> withOptions(MutateInOptions options);
	}

	/**
	 * Fluent method to specify the collection.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface MutateInByIdInCollection<T> extends MutateInByIdWithOptions<T>, InCollection<Object> {
		/**
		 * With a different collection
		 *
		 * @param collection the collection to use.
		 */
		@Override
		MutateInByIdWithOptions<T> inCollection(String collection);
	}

	/**
	 * Fluent method to specify the scope.
	 *
	 * @param <T> the entity type to use for the results.
	 */
	interface MutateInByIdInScope<T> extends MutateInByIdInCollection<T>, InScope<Object> {
		/**
		 * With a different scope
		 *
		 * @param scope the scope to use.
		 */
		@Override
		MutateInByIdInCollection<T> inScope(String scope);
	}

	interface MutateInByIdWithDurability<T> extends MutateInByIdInScope<T>, WithDurability<T> {
		@Override
		MutateInByIdInScope<T> withDurability(DurabilityLevel durabilityLevel);

		@Override
		MutateInByIdInScope<T> withDurability(PersistTo persistTo, ReplicateTo replicateTo);

	}

	interface MutateInByIdWithExpiry<T> extends MutateInByIdWithDurability<T>, WithExpiry<T> {
		@Override
		MutateInByIdWithDurability<T> withExpiry(Duration expiry);
	}

	/**
	 * Provides methods for constructing KV operations in a fluent way.
	 *
	 * @param <T> the entity type to upsert
	 */
	interface ReactiveMutateInById<T> extends MutateInByIdWithExpiry<T> {}

}
