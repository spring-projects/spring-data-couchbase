package org.springframework.data.couchbase.core;

import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface ExecutableQueryOperation {

  <T> ExecutableQuery<T> query(Class<T> domainType);

  interface TerminatingQuery<T> {
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

  }

  interface ExecutableQuery<T> extends TerminatingQuery<T> {}

}
