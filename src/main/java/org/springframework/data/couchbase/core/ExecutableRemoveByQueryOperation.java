package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.core.query.Query;
import reactor.core.publisher.Flux;

import java.util.List;

public interface ExecutableRemoveByQueryOperation {

  <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType);

  interface TerminatingRemoveByQuery<T> {

    List<RemoveResult> all();

    TerminatingReactiveRemoveByQuery reactive();

  }

  interface TerminatingReactiveRemoveByQuery<T> {

    Flux<RemoveResult> all();

  }

  interface RemoveByQueryWithQuery<T> extends TerminatingRemoveByQuery<T> {

    TerminatingRemoveByQuery<T> matching(Query query);

  }

  interface ExecutableRemoveByQuery<T> extends RemoveByQueryWithQuery<T> {}

}
