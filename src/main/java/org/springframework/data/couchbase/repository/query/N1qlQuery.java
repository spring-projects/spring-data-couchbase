package org.springframework.data.couchbase.repository.query;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

public class N1qlQuery implements RepositoryQuery {

  private final CouchbaseOperations couchbaseOperations;

  public N1qlQuery(final CouchbaseOperations couchbaseOperations) {
    this.couchbaseOperations = couchbaseOperations;
  }

  @Override
  public Object execute(Object[] parameters) {
    return null;
  }

  @Override
  public QueryMethod getQueryMethod() {
    return null;
  }

}
