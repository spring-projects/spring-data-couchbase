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
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * Factory to create {@link CouchbaseRepository} instances.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryFactory extends RepositoryFactorySupport {

  private final CouchbaseOperations couchbaseOperations;
  private final MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> mappingContext;

  public CouchbaseRepositoryFactory(final CouchbaseOperations couchbaseOperations) {
    Assert.notNull(couchbaseOperations);
    this.couchbaseOperations = couchbaseOperations;
    mappingContext = couchbaseOperations.getConverter().getMappingContext();
  }

  @Override
  public <T, ID extends Serializable> CouchbaseEntityInformation<T, ID>
    getEntityInformation(Class<T> domainClass) {
    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

    if(entity == null) {
      throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!",
        domainClass.getName()));
    }
    return new MappingCouchbaseEntityInformation<T, ID>((CouchbasePersistentEntity<T>) entity);
  }

  @Override
  protected Object getTargetRepository(RepositoryMetadata metadata) {
    CouchbaseEntityInformation<?, Serializable> entityInformation =
      getEntityInformation(metadata.getDomainType());
    return new SimpleCouchbaseRepository(entityInformation, couchbaseOperations);
  }

  @Override
  protected Class<?> getRepositoryBaseClass(RepositoryMetadata repositoryMetadata) {
    return SimpleCouchbaseRepository.class;
  }
}
