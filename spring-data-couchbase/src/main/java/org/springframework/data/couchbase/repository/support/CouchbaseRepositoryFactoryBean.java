/*
 * Copyright 2013-2020 the original author or authors.
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

package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * The factory bean to create repositories.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	/**
	 * Contains the reference to the template.
	 */
	private RepositoryOperationsMapping operationsMapping;

	/**
	 * Creates a new {@link CouchbaseRepositoryFactoryBean} for the given repository interface.
	 * 
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public CouchbaseRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/**
	 * Set the template reference.
	 *
	 * @param operations the reference to the operations template.
	 */
	public void setCouchbaseOperations(final CouchbaseOperations operations) {
		setCouchbaseOperationsMapping(new RepositoryOperationsMapping(operations));
	}

	public void setCouchbaseOperationsMapping(final RepositoryOperationsMapping mapping) {
		this.operationsMapping = mapping;
		setMappingContext(operationsMapping.getMappingContext());
	}

	/**
	 * Returns a factory instance.
	 *
	 * @return the factory instance.
	 */
	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		return getFactoryInstance(operationsMapping);
	}

	/**
	 * Get the factory instance for the operations.
	 *
	 * @param operationsMapping the reference to the template.
	 * @return the factory instance.
	 */
	protected CouchbaseRepositoryFactory getFactoryInstance(final RepositoryOperationsMapping operationsMapping) {
		return new CouchbaseRepositoryFactory(operationsMapping);
	}

	/**
	 * Make sure that the dependencies are set and not null.
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.notNull(operationsMapping, "operationsMapping must not be null!");
	}
}
