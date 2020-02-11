package org.springframework.data.couchbase.core;

import java.util.Collection;

public interface ExecutableGetOperation {

  <T> ExecutableGet<T> get(Class<T> domainType);

  interface TerminatingGet<T> {

    T one(String id);

    Collection<? extends T> all(Collection<String> ids);

  }

  interface GetWithCollection<T> extends TerminatingGet<T> {

    TerminatingGet<T> inCollection(String collection);
  }

  interface GetWithProjection<T> extends GetWithCollection<T> {

    GetWithCollection<T> project(String... fields);

  }

  interface ExecutableGet<T> extends GetWithProjection<T> {}

}
