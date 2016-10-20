package org.springframework.data.couchbase.repository.base.impl;

import java.io.Serializable;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.N1qlCouchbaseRepository;

public class MyRepositoryImpl<T, ID extends Serializable>
    extends N1qlCouchbaseRepository<T, ID>
    implements MyRepository<T, ID> {

  public MyRepositoryImpl(CouchbaseEntityInformation<T, String> metadata, CouchbaseOperations couchbaseOperations) {
    super(metadata, couchbaseOperations);
  }

  @Override
  public int sharedCustomMethod(ID id) {
    String key = String.valueOf(id);
    if (key.startsWith("a"))
      return key.length() * 1000;
    return key.length();
  }
}
