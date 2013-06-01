package org.springframework.data.couchbase.repository.support;


import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

import java.io.Serializable;

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
