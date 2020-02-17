package org.springframework.data.couchbase.core;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

public interface ExecutableExistsByIdOperation {

  ExecutableExistsById existsById();

  interface TerminatingExistsById {

    boolean one(String id);

    Map<String, Boolean> all(Collection<String> ids);

    TerminatingReactiveExistsById reactive();

  }

  interface TerminatingReactiveExistsById {

    Mono<Boolean> one(String id);

    Mono<Map<String, Boolean>> all(Collection<String> ids);

  }

  interface ExistsByIdWithCollection extends TerminatingExistsById {

    TerminatingExistsById inCollection(String collection);
  }

  interface ExecutableExistsById extends ExistsByIdWithCollection {}

}
