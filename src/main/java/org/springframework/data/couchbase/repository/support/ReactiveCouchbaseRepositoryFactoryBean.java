/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.support;

import java.io.Serializable;

import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
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
     * Contains the reference to the IndexManager.
     */
    private IndexManager indexManager;

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
     * @param couchbaseOperationsMapping the reference to the operations template.
     */
    public void setCouchbaseOperations(final RxJavaCouchbaseOperations couchbaseOperationsMapping) {
        setCouchbaseOperationsMapping(new ReactiveRepositoryOperationsMapping(couchbaseOperationsMapping));
    }

    public void setCouchbaseOperationsMapping(final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping) {
        this.couchbaseOperationsMapping = couchbaseOperationsMapping;
        setMappingContext(couchbaseOperationsMapping.getMappingContext());
    }

    /**
     * Set the IndexManager reference.
     *
     * @param indexManager the IndexManager to use.
     */
    public void setIndexManager(final IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    /**
     * Returns a factory instance.
     *
     * @return the factory instance.
     */
    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        return getFactoryInstance(couchbaseOperationsMapping, indexManager);
    }

    /**
     * Get the factory instance for the operations.
     *
     * @param couchbaseOperationsMapping the reference to the template.
     * @param indexManager the reference to the {@link IndexManager}.
     * @return the factory instance.
     */
    protected ReactiveCouchbaseRepositoryFactory getFactoryInstance(final ReactiveRepositoryOperationsMapping couchbaseOperationsMapping,
                                                            IndexManager indexManager) {
        return new ReactiveCouchbaseRepositoryFactory(couchbaseOperationsMapping, indexManager);
    }

    /**
     * Make sure that the dependencies are set and not null.
     */
    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        Assert.notNull(couchbaseOperationsMapping, "operationsMapping must not be null!");
        Assert.notNull(indexManager, "indexManager must not be null!");
    }
}
