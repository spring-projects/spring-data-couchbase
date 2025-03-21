/*
 * Copyright 2014-2025 the original author or authors.
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
package org.springframework.data.couchbase.repository.cdi;

import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.config.CustomRepositoryImplementationDetector;
import org.springframework.util.Assert;

/**
 * A bean which represents a Couchbase repository.
 *
 * @author Mark Paluch
 */
public class CouchbaseRepositoryBean<T> extends CdiRepositoryBean<T> {

	private final Bean<CouchbaseOperations> couchbaseOperationsBean;

	/**
	 * Creates a new {@link CouchbaseRepositoryBean}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param qualifiers must not be {@literal null}.
	 * @param repositoryType must not be {@literal null}.
	 * @param beanManager must not be {@literal null}.
	 * @param detector detector for the custom {@link org.springframework.data.repository.Repository} implementations
	 *          {@link org.springframework.data.repository.config.CustomRepositoryImplementationDetector}, can be
	 *          {@literal null}.
	 */
	public CouchbaseRepositoryBean(Bean<CouchbaseOperations> operations, Set<Annotation> qualifiers,
			Class<T> repositoryType, BeanManager beanManager, CustomRepositoryImplementationDetector detector) {
		super(qualifiers, repositoryType, beanManager, Optional.of(detector));

		Assert.notNull(operations, "Cannot create repository with 'null' for CouchbaseOperations.");
		this.couchbaseOperationsBean = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.cdi.CdiRepositoryBean#create(javax.enterprise.context.spi.CreationalContext, java.lang.Class)
	*/
	@Override
	protected T create(jakarta.enterprise.context.spi.CreationalContext<T> creationalContext, Class<T> repositoryType) {
		CouchbaseOperations couchbaseOperations = getDependencyInstance(couchbaseOperationsBean, CouchbaseOperations.class);
		RepositoryOperationsMapping couchbaseOperationsMapping = new RepositoryOperationsMapping(couchbaseOperations);

		return create(() -> new CouchbaseRepositoryFactory(couchbaseOperationsMapping), repositoryType);
	}

	@Override
	public Class<? extends Annotation> getScope() {
		return couchbaseOperationsBean.getScope();
	}

}
