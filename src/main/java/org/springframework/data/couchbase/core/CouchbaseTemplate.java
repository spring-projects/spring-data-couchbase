package org.springframework.data.couchbase.core;

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;

public class CouchbaseTemplate implements CouchbaseOperations {

  private final CouchbaseClientFactory clientFactory;
  private final CouchbaseConverter converter;
  private final PersistenceExceptionTranslator exceptionTranslator;
  private final CouchbaseTemplateSupport templateSupport;

  public CouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
    this.clientFactory = clientFactory;
    this.converter = converter;
    this.exceptionTranslator = clientFactory.getExceptionTranslator();
    this.templateSupport = new CouchbaseTemplateSupport(converter);
  }

  @Override
  public <T> ExecutableUpsert<T> upsert(final Class<T> domainType) {
    return new ExecutableUpsertOperationSupport(this).upsert(domainType);
  }

  @Override
  public <T> ExecutableGet<T> get(Class<T> domainType) {
    return new ExecutableGetOperationSupport(this).get(domainType);
  }

  @Override
  public <T> ExecutableQuery<T> query(Class<T> domainType) {
    return new ExecutableQueryOperationSupport(this).query(domainType);
  }

  @Override
  public String getBucketName() {
    return clientFactory.getBucket().name();
  }

  @Override
  public String getScopeName() {
    return clientFactory.getScope().name();
  }

  @Override
  public CouchbaseClientFactory getCouchbaseClientFactory() {
    return clientFactory;
  }

  /**
   * Provides access to a  {@link Collection} on the configured {@link CouchbaseClientFactory}.
   *
   * @param collectionName the name of the collection, if null is passed in the default collection is assumed.
   * @return the collection instance.
   */
  public Collection getCollection(final String collectionName) {
    final Scope scope = clientFactory.getScope();
    if (collectionName == null) {
      if (!scope.name().equals(CollectionIdentifier.DEFAULT_SCOPE)) {
        throw new IllegalStateException("A collectionName must be provided if a non-default scope is used!");
      }
      return clientFactory.getBucket().defaultCollection();
    }
    return scope.collection(collectionName);
  }

  @Override
  public CouchbaseConverter getConverter() {
    return converter;
  }


  CouchbaseTemplateSupport support() {
    return templateSupport;
  }

  /**
   * Tries to convert the given {@link RuntimeException} into a {@link DataAccessException} but returns the original
   * exception if the conversation failed. Thus allows safe re-throwing of the return value.
   *
   * @param ex the exception to translate
   */
  RuntimeException potentiallyConvertRuntimeException(RuntimeException ex) {
    RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(ex);
    return resolved == null ? ex : resolved;
  }

}
