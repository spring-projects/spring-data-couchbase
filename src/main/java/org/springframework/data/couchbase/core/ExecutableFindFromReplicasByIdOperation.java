package org.springframework.data.couchbase.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ExecutableFindFromReplicasByIdOperation {

  <T> ExecutableFindFromReplicasById<T> findFromReplicasById(Class<T> domainType);

  interface TerminatingFindFromReplicasById<T> {

    T any(String id);

    Collection<? extends T> any(Collection<String> ids);

    TerminatingReactiveFindFromReplicasById<T> reactive();

  }

  interface TerminatingReactiveFindFromReplicasById<T> {

    Mono<T> any(String id);

    Flux<? extends T> any(Collection<String> ids);

  }

  interface FindFromReplicasByIdWithCollection<T> extends TerminatingFindFromReplicasById<T> {

    TerminatingFindFromReplicasById<T> inCollection(String collection);
  }


  interface ExecutableFindFromReplicasById<T> extends FindFromReplicasByIdWithCollection<T> {}

}
