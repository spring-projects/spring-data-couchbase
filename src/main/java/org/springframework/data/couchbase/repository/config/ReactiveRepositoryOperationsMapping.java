/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.config;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveRepositoryOperationsMapping {
	private CouchbaseOperations defaultOperations;
	private Map<String, CouchbaseOperations> byRepository = new HashMap<>();
	private Map<String, CouchbaseOperations> byEntity = new HashMap<>();

	/**
	 * Creates a new mapping, setting the default fallback to use by otherwise non mapped repositories.
	 *
	 * @param defaultOperations the default fallback reactive couchbase operations.
	 */
	public ReactiveRepositoryOperationsMapping(CouchbaseOperations defaultOperations) {
		Assert.notNull(defaultOperations);
		this.defaultOperations = defaultOperations;
	}

	/**
	 * Change the default reactive couchbase operations in an existing mapping.
	 *
	 * @param aDefault the new default couchbase operations.
	 * @return the mapping, for chaining.
	 */
	public ReactiveRepositoryOperationsMapping setDefault(CouchbaseOperations aDefault) {
		Assert.notNull(aDefault);
		this.defaultOperations = aDefault;
		return this;
	}

	/**
	 * Add a highest priority mapping that will associate a specific repository interface with a given
	 * {@link CouchbaseOperations}.
	 *
	 * @param repositoryInterface the repository interface {@link Class}.
	 * @param operations the ReactiveCouchbaseOperations to use.
	 * @return the mapping, for chaining.
	 */
	public ReactiveRepositoryOperationsMapping map(Class<?> repositoryInterface, CouchbaseOperations operations) {
		byRepository.put(repositoryInterface.getName(), operations);
		return this;
	}

	/**
	 * Add a middle priority mapping that will associate any un-mapped repository that deals with the given domain type
	 * Class with a given {@link CouchbaseOperations}.
	 *
	 * @param entityClass the domain type's {@link Class}.
	 * @param operations the CouchbaseOperations to use.
	 * @return the mapping, for chaining.
	 */
	public ReactiveRepositoryOperationsMapping mapEntity(Class<?> entityClass, CouchbaseOperations operations) {
		byEntity.put(entityClass.getName(), operations);
		return this;
	}

	/**
	 * @return the configured default {@link CouchbaseOperations}.
	 */
	public CouchbaseOperations getDefault() {
		return defaultOperations;
	}

	/**
	 * Get the {@link MappingContext} to use in repositories. It is extracted from the default {@link CouchbaseOperations}.
	 *
	 *  @return the mapping context.
	 */
	public MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> getMappingContext() {
		return defaultOperations.getConverter().getMappingContext();
	}

	/**
	 * Given a repository interface and its domain type, resolves which {@link CouchbaseOperations} it should be backed with.
	 *
	 * Starts by looking for a direct mapping to the interface, then a common mapping for the domain type, then falls back
	 * to the default CouchbaseOperations.
	 *
	 * @param repositoryInterface the repository's interface.
	 * @param domainType the repository's domain type / entity.
	 * @return the CouchbaseOperations to back the repository.
	 */
	public CouchbaseOperations resolve(Class<?> repositoryInterface, Class<?> domainType) {
		CouchbaseOperations result = byRepository.get(repositoryInterface.getName());
		if (result != null) {
			return result;
		} else {
			result = byEntity.get(domainType.getName());
			if (result != null) {
				return result;
			} else {
				return defaultOperations;
			}
		}
	}
}

