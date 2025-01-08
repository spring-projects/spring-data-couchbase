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

package org.springframework.data.couchbase.repository.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * A utility class for configuration allowing to tell which {@link CouchbaseOperations} should be backing repositories.
 * Its fluent API allows to set that (in order of precedence) for specific repository interfaces, by repository domain
 * type or as a default fallback.
 *
 * @author Simon Baslé
 * @author Mark Paluch
 */
public class RepositoryOperationsMapping {
	private CouchbaseOperations defaultOperations;
	private Map<String, CouchbaseOperations> byRepository = new HashMap<>();
	private Map<String, CouchbaseOperations> byEntity = new HashMap<>();

	/**
	 * Creates a new mapping, setting the default fallback to use by otherwise non mapped repositories.
	 *
	 * @param defaultOperations the default fallback couchbase operations.
	 */
	public RepositoryOperationsMapping(CouchbaseOperations defaultOperations) {
		Assert.notNull(defaultOperations, "CouchbaseOperations must not be null!");
		this.defaultOperations = defaultOperations;
	}

	/**
	 * Add a highest priority mapping that will associate a specific repository interface with a given
	 * {@link CouchbaseOperations}.
	 *
	 * @param repositoryInterface the repository interface {@link Class}.
	 * @param couchbaseOperations the CouchbaseOperations to use.
	 * @return the mapping, for chaining.
	 */
	public RepositoryOperationsMapping map(Class<?> repositoryInterface, CouchbaseOperations couchbaseOperations) {
		byRepository.put(repositoryInterface.getName(), couchbaseOperations);
		return this;
	}

	/**
	 * Add a middle priority mapping that will associate any un-mapped repository that deals with the given domain type
	 * Class with a given {@link CouchbaseOperations}.
	 *
	 * @param entityClass the domain type's {@link Class}.
	 * @param couchbaseOperations the CouchbaseOperations to use.
	 * @return the mapping, for chaining.
	 */
	public RepositoryOperationsMapping mapEntity(Class<?> entityClass, CouchbaseOperations couchbaseOperations) {
		byEntity.put(entityClass.getName(), couchbaseOperations);
		return this;
	}

	/**
	 * @return the configured default {@link CouchbaseOperations}.
	 */
	public CouchbaseOperations getDefault() {
		return defaultOperations;
	}

	/**
	 * Change the default couchbase operations in an existing mapping.
	 *
	 * @param aDefault the new default couchbase operations.
	 * @return the mapping, for chaining.
	 */
	public RepositoryOperationsMapping setDefault(CouchbaseOperations aDefault) {
		Assert.notNull(aDefault, "CouchbaseOperations must not be null!");
		this.defaultOperations = aDefault;
		return this;
	}

	/**
	 * Get the {@link MappingContext} to use in repositories. It is extracted from the default
	 * {@link CouchbaseOperations}.
	 *
	 * @return the mapping context.
	 */
	public MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> getMappingContext() {
		return defaultOperations.getConverter().getMappingContext();
	}

	/**
	 * Given a repository interface and its domain type, resolves which {@link CouchbaseOperations} it should be backed
	 * with. Starts by looking for a direct mapping to the interface, then a common mapping for the domain type, then
	 * falls back to the default CouchbaseOperations.
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
