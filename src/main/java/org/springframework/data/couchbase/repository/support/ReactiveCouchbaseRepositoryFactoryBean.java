/*
 * Copyright 2017-present the original author or authors.
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

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public class ReactiveCouchbaseRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	/**
	 * Contains the reference to the template.
	 */
	private ReactiveRepositoryOperationsMapping couchbaseOperationsMapping;

	/**
	 * Creates a new {@link CouchbaseRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public ReactiveCouchbaseRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/**
	 * Set the template reference.
	 *
	 * @param reactiveCouchbaseOperations the reference to the operations template.
	 */
	public void setCouchbaseOperations(final ReactiveCouchbaseOperations reactiveCouchbaseOperations) {
		setReactiveCouchbaseOperationsMapping(new ReactiveRepositoryOperationsMapping(reactiveCouchbaseOperations));
	}

	public void setReactiveCouchbaseOperationsMapping(
			final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping) {
		this.couchbaseOperationsMapping = couchbaseOperationsMapping;
		setMappingContext(couchbaseOperationsMapping.getMappingContext());
	}

	/**
	 * Returns a factory instance.
	 *
	 * @return the factory instance.
	 */
	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		return getFactoryInstance(couchbaseOperationsMapping);
	}

	/**
	 * Get the factory instance for the operations.
	 *
	 * @param couchbaseOperationsMapping the reference to the template.
	 * @return the factory instance.
	 */
	protected ReactiveCouchbaseRepositoryFactory getFactoryInstance(
			final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping) {
		return new ReactiveCouchbaseRepositoryFactory(couchbaseOperationsMapping);
	}

	/**
	 * Make sure that the dependencies are set and not null.
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.notNull(couchbaseOperationsMapping, "operationsMapping must not be null!");
	}
}
