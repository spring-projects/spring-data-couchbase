package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.core.query.Query;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface ExecutableFindByQueryOperation {

  <T> ExecutableFindByQuery<T> findByQuery(Class<T> domainType);

  interface TerminatingFindByQuery<T> {
    /**
     * Get exactly zero or one result.
     *
     * @return {@link Optional#empty()} if no match found.
     * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
     */
    default Optional<T> one() {
      return Optional.ofNullable(oneValue());
    }

    /**
     * Get exactly zero or one result.
     *
     * @return {@literal null} if no match found.
     * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
     */
    @Nullable
    T oneValue();

    /**
     * Get the first or no result.
     *
     * @return {@link Optional#empty()} if no match found.
     */
    default Optional<T> first() {
      return Optional.ofNullable(firstValue());
    }

    /**
     * Get the first or no result.
     *
     * @return {@literal null} if no match found.
     */
    @Nullable
    T firstValue();

    /**
     * Get all matching elements.
     *
     * @return never {@literal null}.
     */
    List<T> all();

    /**
     * Stream all matching elements.
     *
     * @return a {@link Stream} of results. Never {@literal null}.
     */
    Stream<T> stream();

    /**
     * Get the number of matching elements.
     *
     * @return total number of matching elements.
     */
    long count();

    /**
     * Check for the presence of matching elements.
     *
     * @return {@literal true} if at least one matching element exists.
     */
    boolean exists();


    TerminatingReactiveFindByQuery<T> reactive();

  }

  /**
   * Compose find execution by calling one of the terminating methods.
   */
  interface TerminatingReactiveFindByQuery<T> {

    Mono<T> one();

    Mono<T> first();

    Flux<T> all();

    Flux<T> tail();

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

  interface ExecutableFindByQuery<T> extends FindByQueryWithQuery<T> {}

}
