package org.springframework.data.couchbase.repository;

import org.springframework.data.repository.CrudRepository;

import java.io.Serializable;

/**
 * Couchbase specific {@link org.springframework.data.repository.Repository}
 * interface.
 */
public interface CouchbaseRepository<T, ID extends Serializable> extends CrudRepository<T, ID> {
}
