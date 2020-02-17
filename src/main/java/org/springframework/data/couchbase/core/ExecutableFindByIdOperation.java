package org.springframework.data.couchbase.core;

import java.util.Collection;

public interface ExecutableFindByIdOperation {

  <T> ExecutableFindById<T> findById(Class<T> domainType);

  interface TerminatingFindById<T> {

    T one(String id);

    Collection<? extends T> all(Collection<String> ids);

  }

  interface FindByIdWithCollection<T> extends TerminatingFindById<T> {

    TerminatingFindById<T> inCollection(String collection);
  }

  interface FindByIdWithProjection<T> extends FindByIdWithCollection<T> {

    FindByIdWithCollection<T> project(String... fields);

  }

  interface ExecutableFindById<T> extends FindByIdWithProjection<T> {}

}
