/*
 * Copyright 2013 the original author or authors.
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

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * The factory bean to create repositories.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends
  RepositoryFactoryBeanSupport<T, S, ID> {

  /**
   * Contains the reference to the template.
   */
  private CouchbaseOperations operations;

  /**
   * Set the template reference.
   *
   * @param operations the reference to the operations template.
   */
  public void setCouchbaseOperations(final CouchbaseOperations operations) {
    this.operations = operations;
    setMappingContext(operations.getConverter().getMappingContext());
  }

  /**
   * Returns a factory instance.
   *
   * @return the factory instance.
   */
  @Override
  protected RepositoryFactorySupport createRepositoryFactory() {
    return getFactoryInstance(operations);
  }

  /**
   * Get the factory instance for the operations.
   *
   * @param operations the reference to the template.
   * @return the factory instance.
   */
  private RepositoryFactorySupport getFactoryInstance(final CouchbaseOperations operations) {
    return new CouchbaseRepositoryFactory(operations);
  }

  /**
   * Make sure that the template is set and not null.
   */
  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();
    Assert.notNull(operations, "CouchbaseTemplate must not be null!");
  }
}
