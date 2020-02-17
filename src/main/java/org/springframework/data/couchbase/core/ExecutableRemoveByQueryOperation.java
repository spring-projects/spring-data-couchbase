package org.springframework.data.couchbase.core;

import com.couchbase.client.java.kv.MutationResult;
import org.springframework.data.couchbase.core.query.Query;

import java.util.List;
import java.util.Map;

public interface ExecutableRemoveByQueryOperation {

  <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType);

  interface TerminatingRemoveByQuery<T> {

    List<RemoveResult> all();

  }

  interface RemoveByQueryWithQuery<T> extends TerminatingRemoveByQuery<T> {

    TerminatingRemoveByQuery<T> matching(Query query);

  }

  interface ExecutableRemoveByQuery<T> extends RemoveByQueryWithQuery<T> {}

}
