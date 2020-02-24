/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface ReactiveFindByQueryOperation {

  <T> ReactiveFindByQuery<T> findByQuery(Class<T> domainType);

  /**
   * Compose find execution by calling one of the terminating methods.
   */
  interface TerminatingFindByQuery<T> {

    Mono<T> one();

    Mono<T> first();

    Flux<T> all();

    Mono<Long> count();

    Mono<Boolean> exists();

  }

  /**
   * Terminating operations invoking the actual query execution.
   *
   * @author Christoph Strobl
   * @since 2.0
   */
  interface FindByQueryWithQuery<T> extends TerminatingFindByQuery<T> {

    /**
     * Set the filter query to be used.
     *
     * @param query must not be {@literal null}.
     * @return new instance of {@link TerminatingFindByQuery}.
     * @throws IllegalArgumentException if query is {@literal null}.
     */
    TerminatingFindByQuery<T> matching(Query query);

  }

  interface FindByQueryConsistentWith<T> extends FindByQueryWithQuery<T> {

    FindByQueryWithQuery<T> consistentWith(QueryScanConsistency scanConsistency);

  }

  interface ReactiveFindByQuery<T> extends FindByQueryConsistentWith<T> {}

}
