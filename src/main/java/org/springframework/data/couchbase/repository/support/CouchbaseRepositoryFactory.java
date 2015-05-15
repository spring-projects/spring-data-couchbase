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
import org.springframework.data.couchbase.repository.query.CouchbaseQueryMethod;
import org.springframework.data.couchbase.repository.query.ViewBasedCouchbaseQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * Factory to create {@link SimpleCouchbaseRepository} instances.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryFactory extends RepositoryFactorySupport {

  /**
   * Holds the reference to the template.
   */
  private final CouchbaseOperations couchbaseOperations;

  /**
   * Holds the mapping context.
   */
  private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

  /**
   * Holds a custom ViewPostProcessor..
   */
  private final ViewPostProcessor viewPostProcessor;

  /**
   * Create a new factory.
   *
   * @param couchbaseOperations the template for the underlying actions.
   */
  public CouchbaseRepositoryFactory(final CouchbaseOperations couchbaseOperations) {
    Assert.notNull(couchbaseOperations);

    this.couchbaseOperations = couchbaseOperations;
    mappingContext = couchbaseOperations.getConverter().getMappingContext();
    viewPostProcessor = ViewPostProcessor.INSTANCE;

    addRepositoryProxyPostProcessor(viewPostProcessor);
  }

  /**
   * Returns entity information based on the domain class.
   *
   * @param domainClass the class for the entity.
   * @param <T> the value type
   * @param <ID> the id type.
   *
   * @return entity information for that domain class.
   */
  @Override
  public <T, ID extends Serializable> CouchbaseEntityInformation<T, ID> getEntityInformation(final Class<T> domainClass) {
    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

    if (entity == null) {
      throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!", domainClass.getName()));
    }

    return new MappingCouchbaseEntityInformation<T, ID>((CouchbasePersistentEntity<T>) entity);
  }

  /**
   * Returns a new Repository based on the metadata.
   *
   * @param metadata the repository metadata.
   *
   * @return a new created repository.
   */
  @Override
  protected Object getTargetRepository(final RepositoryInformation metadata) {
    CouchbaseEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());
    final SimpleCouchbaseRepository simpleCouchbaseRepository = new SimpleCouchbaseRepository(entityInformation, couchbaseOperations);
    simpleCouchbaseRepository.setViewMetadataProvider(viewPostProcessor.getViewMetadataProvider());
    return simpleCouchbaseRepository;
  }

  /**
   * The base class for this repository.
   *
   * @param repositoryMetadata metadata for the repository.
   *
   * @return the base class.
   */
  @Override
  protected Class<?> getRepositoryBaseClass(final RepositoryMetadata repositoryMetadata) {
    return SimpleCouchbaseRepository.class;
  }

  @Override
  protected QueryLookupStrategy getQueryLookupStrategy(QueryLookupStrategy.Key key) {
    return new CouchbaseQueryLookupStrategy();
  }

  /**
   * Currently, only views are supported. N1QL support to be added.
   */
  private class CouchbaseQueryLookupStrategy implements QueryLookupStrategy {
    @Override
    public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, NamedQueries namedQueries) {
      CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, mappingContext);
      return new ViewBasedCouchbaseQuery(queryMethod, couchbaseOperations);
    }
  }

}
