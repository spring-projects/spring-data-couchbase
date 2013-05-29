package com.couchbase.spring.repository.support;


import com.couchbase.spring.core.CouchbaseOperations;
import com.couchbase.spring.core.mapping.CouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;
import com.couchbase.spring.repository.query.CouchbaseEntityInformation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * Factory to create {@link CouchbaseRepository} instances.
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
