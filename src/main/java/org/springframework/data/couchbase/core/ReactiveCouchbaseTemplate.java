package org.springframework.data.couchbase.core;

import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;

public class ReactiveCouchbaseTemplate implements ReactiveCouchbaseOperations {

  private final CouchbaseClientFactory clientFactory;
  private final CouchbaseConverter converter;
  private final PersistenceExceptionTranslator exceptionTranslator;
  private final CouchbaseTemplateSupport templateSupport;

  public ReactiveCouchbaseTemplate(final CouchbaseClientFactory clientFactory, final CouchbaseConverter converter) {
    this.clientFactory = clientFactory;
    this.converter = converter;
    this.exceptionTranslator = clientFactory.getExceptionTranslator();
    this.templateSupport = new CouchbaseTemplateSupport(converter);
  }

  @Override
  public CouchbaseConverter getConverter() {
    return converter;
  }

  @Override
  public String getBucketName() {
    return clientFactory.getBucket().name();
  }

  @Override
  public String getScopeName() {
    return clientFactory.getScope().name();
  }
}
