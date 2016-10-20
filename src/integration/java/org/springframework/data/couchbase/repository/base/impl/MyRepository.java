package org.springframework.data.couchbase.repository.base.impl;

import java.io.Serializable;

import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
public interface MyRepository<T, ID extends Serializable> extends CouchbaseRepository<T, ID> {

  int sharedCustomMethod(ID id);
}
