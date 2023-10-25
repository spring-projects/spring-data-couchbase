package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

import java.io.Serializable;

public class MyReactiveCouchbaseRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends ReactiveCouchbaseRepositoryFactoryBean<T, S, ID> {

  /**
   * Contains the reference to the template.
   */
  private ReactiveRepositoryOperationsMapping couchbaseOperationsMapping;
  /**
   * Contains the reference to the template.
   */
  private ReactiveRepositoryOperationsMapping couchbaseOperationsMappingFallback;
  /**
   * Creates a new {@link CouchbaseRepositoryFactoryBean} for the given repository interface.
   *
   * @param repositoryInterface must not be {@literal null}.
   */
  public MyReactiveCouchbaseRepositoryFactoryBean(Class repositoryInterface) {
    super(repositoryInterface);
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
    return new ReactiveCouchbaseRepositoryFactory(couchbaseOperationsMapping, couchbaseOperationsMappingFallback);
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
    super.setReactiveCouchbaseOperationsMapping(couchbaseOperationsMapping);
    this.couchbaseOperationsMapping = couchbaseOperationsMapping;
    setMappingContext(couchbaseOperationsMapping.getMappingContext());
    this.couchbaseOperationsMappingFallback = couchbaseOperationsMapping;
    setMappingContext(couchbaseOperationsMapping.getMappingContext());
  }
  /**
   * Make sure that the dependencies are set and not null.
   */
  //@Override
  //public void afterPropertiesSet() {
  //  super.afterPropertiesSet();
  //  Assert.notNull(couchbaseOperationsMapping, "operationsMapping must not be null!");
  //}
}
