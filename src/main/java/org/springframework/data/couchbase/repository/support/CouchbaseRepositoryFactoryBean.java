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
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends
  RepositoryFactoryBeanSupport<T, S, ID> {

  private CouchbaseOperations operations;

  public void setCouchbaseOperations(CouchbaseOperations operations) {
    this.operations = operations;
    setMappingContext(operations.getConverter().getMappingContext());
  }

  @Override
  protected RepositoryFactorySupport createRepositoryFactory() {
    return getFactoryInstance(operations);
  }

  private RepositoryFactorySupport getFactoryInstance(CouchbaseOperations operations) {
    return new CouchbaseRepositoryFactory(operations);
  }

  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();
    Assert.notNull(operations, "CouchbaseTemplate must not be null!");
  }
}
