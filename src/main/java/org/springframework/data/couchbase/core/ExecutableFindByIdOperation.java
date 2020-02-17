package org.springframework.data.couchbase.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ExecutableFindByIdOperation {

  <T> ExecutableFindById<T> findById(Class<T> domainType);

  interface TerminatingFindById<T> {

    T one(String id);

    Collection<? extends T> all(Collection<String> ids);

    TerminatingReactiveFindById<T> reactive();

  }

  interface TerminatingReactiveFindById<T> {

    Mono<T> one(String id);

    Flux<? extends T> all(Collection<String> ids);

  }

  interface FindByIdWithCollection<T> extends TerminatingFindById<T> {

    TerminatingFindById<T> inCollection(String collection);
  }

  interface FindByIdWithProjection<T> extends FindByIdWithCollection<T> {

    FindByIdWithCollection<T> project(String... fields);

  }

  interface ExecutableFindById<T> extends FindByIdWithProjection<T> {}

}
